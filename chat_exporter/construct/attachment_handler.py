from asyncio.log import logger
import datetime
import io
import os
import pathlib
from typing import List, Union, Optional, TYPE_CHECKING
import urllib.parse
from PIL import Image

from aiobotocore.session import get_session
from dotenv import load_dotenv

import aiohttp
from chat_exporter.ext.aiohttp_factory import ClientSessionFactory
from chat_exporter.ext.discord_import import discord

if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client


class AttachmentHandler:
    """Handle the saving of attachments (images, videos, audio, etc.)

    Subclass this to implement your own asset handler."""

    async def process_asset(self, attachment: discord.Attachment) -> discord.Attachment:
        """Implement this to process the asset and return a url to the stored attachment.
        :param attachment: discord.Attachment
        :return: str
        """
        raise NotImplementedError


class AttachmentToLocalFileHostHandler(AttachmentHandler):
    """Save the assets to a local file host and embed the assets in the transcript from there."""

    def __init__(
        self,
        base_path: Union[str, pathlib.Path],
        url_base: str,
        compress_amount: Optional[int] = None,
    ):
        if isinstance(base_path, str):
            base_path = pathlib.Path(base_path)
        self.base_path = base_path
        self.url_base = url_base
        self.compress_amount = compress_amount

    async def process_asset(self, attachment: discord.Attachment) -> discord.Attachment:
        """Implement this to process the asset and return a url to the stored attachment.
        :param attachment: discord.Attachment
        :return: str
        """
        file_name = urllib.parse.quote_plus(
            f"{datetime.datetime.utcnow().timestamp()}_{attachment.filename}"
        )
        valid_image_exts = [
            ".png",
            ".jpg",
            ".jpeg",
            ".webp",
            ".bmp",
            ".tiff",
            ".gif",
        ]  # animated gifs will be converted to a single frame jpeg if even supported??
        is_valid = any(
            attachment.filename.lower().endswith(ext) for ext in valid_image_exts
        )
        if self.compress_amount is not None and is_valid:
            try:
                session = await ClientSessionFactory.create_or_get_session()
                async with session.get(attachment.url) as res:
                    if res.status != 200:
                        res.raise_for_status()
                    data = io.BytesIO(await res.read())
                    data.seek(0)
                    image = Image.open(data)
                    rgb_image = image.convert("RGB")
                    compressed_path = self.base_path / file_name
                    rgb_image.save(
                        compressed_path, format="JPEG", quality=self.compress_amount
                    )  # compress it down using jpeg compressor, works even with .png
            except Exception as e:
                # fall back to not compressing..
                pass
        else:
            asset_path = self.base_path / file_name
            await attachment.save(asset_path)

        file_url = f"{self.url_base}/{file_name}"
        attachment.url = file_url
        attachment.proxy_url = file_url
        return attachment


class AttachmentToDiscordChannelHandler(AttachmentHandler):
    """Save the attachment to a discord channel and embed the assets in the transcript from there."""

    def __init__(self, channel: discord.TextChannel):
        self.channel = channel

    async def process_asset(self, attachment: discord.Attachment) -> discord.Attachment:
        """Implement this to process the asset and return a url to the stored attachment.
        :param attachment: discord.Attachment
        :return: str
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(attachment.url) as res:
                    if res.status != 200:
                        res.raise_for_status()
                    data = io.BytesIO(await res.read())
                    data.seek(0)
                    attach = discord.File(data, attachment.filename)
                    msg: discord.Message = await self.channel.send(file=attach)
                    return msg.attachments[0]
        except discord.errors.HTTPException as e:
            # discords http errors, including missing permissions
            raise e


class AsyncS3ClientManager(AttachmentHandler):
    """Singleton factory for aiobotocore Cloudflare R2 client.
    Auto obtains from environment variables:
    - AWS_ENDPOINT_URL
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    """

    load_dotenv()
    endpoint_url = os.getenv("AWS_ENDPOINT_URL")
    r2_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    r2_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    _client = None
    _context = None

    @classmethod
    async def get_client(cls):
        """Get or create the singleton context manager interface."""
        if cls._client is None:
            session = get_session()
            cls._context = session.create_client(
                service_name="s3",
                endpoint_url=cls.endpoint_url,
                aws_access_key_id=cls.r2_access_key_id,
                aws_secret_access_key=cls.r2_secret_access_key,
            )
            cls._client = await cls._context.__aenter__()
        if not cls._client:
            raise ConnectionError(
                "Could not obtain S3 client - Check your environment variables."
            )
        return cls._client

    @classmethod
    async def close(cls):
        """Close the singleton client."""
        if cls._context is not None:
            await cls._context.__aexit__(None, None, None)
            cls._client = None
            cls._context = None


class S3Manager:
    def __init__(
        self, aiobotocore_s3_client: Optional["S3Client"], bucket: str = "bald-server"
    ):
        self.bucket = bucket
        self.client = aiobotocore_s3_client
        self.s3 = None

    async def get_all_keys(self) -> List[str]:
        if self.client is None:
            self.client = await AsyncS3ClientManager.get_client()

        response = await self.client.list_objects_v2(Bucket=self.bucket)
        objects = []
        if "Contents" in response:
            for obj in response["Contents"]:
                if "Key" in obj:
                    objects.append(obj["Key"])
        return objects

    async def upload_file_data(
        self,
        data: Union[bytes, io.BytesIO],
        key_name: str,
        overwrite: bool = False,
        content_type: str = "",
        skip_files_which_are_too_large: bool = False,
    ) -> None:
        """Upload a file-like object or bytes to Cloudflare R2/ s3 with ability to overwrite or not.

        RAISES:
        >>> FileExistsError # if overwrite is False and file exists.
        >>> ValueError # if file size exceeds 25MB.
        >>> KeyError # if unable to determine if object exists.
        >>> TypeError # if data is not bytes or io.BytesIO.
        """

        if not isinstance(data, (bytes, io.BytesIO)):
            raise TypeError("Warning: Attachment data must be bytes or io.BytesIO")
        if isinstance(data, io.BytesIO):
            data.seek(0)
        if _get_data_size(data) > 25 * 1024 * 1024:
            if skip_files_which_are_too_large:
                logger.warning(f"File {key_name} exceeds 25MB limit, skipping upload.")
                return
            raise ValueError("File size exceeds 25MB limit.")

        if self.client is None:
            self.client = await AsyncS3ClientManager.get_client()
        try:
            if not overwrite:
                obj = await self.client.get_object(Bucket=self.bucket, Key=key_name)
                if obj is not None:
                    try:
                        status = obj["ResponseMetadata"]["HTTPStatusCode"]
                    except KeyError:
                        logger.warning(
                            "Could not determine if object exists, blocking upload."
                        )
                        return
                    if status == 200:
                        raise FileExistsError(
                            f"Object with key {key_name} already exists in bucket {self.bucket}."
                        )  # raise bespoke error?

        except self.client.exceptions.NoSuchKey:
            pass  # we can proceed to upload as the object does not exist

        await self.client.put_object(
            Body=data, Bucket=self.bucket, Key=key_name, ContentType=content_type
        )

    async def delete_files_in_list(self, files_to_delete: List[str]) -> None:
        """Delete all files from Cloudflare R2/s3 that start with the given prefix.

        Can Raise AssertionError if deletion fails."""
        if self.client is None:
            self.client = await AsyncS3ClientManager.get_client()

        logger.info(f"Deleting files with from bucket {self.bucket}")
        keys_to_delete = []

        for file_key in files_to_delete:
            if len(keys_to_delete) >= 1000:
                break  # will loop again to delete more
            try:
                obj = await self.client.get_object(Bucket=self.bucket, Key=file_key)
            except self.client.exceptions.NoSuchKey:
                continue  # file does not exist, skip
            if obj is not None:
                keys_to_delete.append({"Key": file_key})

        if keys_to_delete:
            deleted_response = await self.client.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": [{"Key": obj["Key"]} for obj in keys_to_delete]},
            )
            logger.debug(f"Deleted objects response: {deleted_response}")
            if len(keys_to_delete) > 1000:
                # Cloudflare R2/s3 delete_objects can only delete up to 1000 objects at a time so recursively call.
                logger.debug(f"Recursively deleting more than 1000 objects")
                await self.delete_files_in_list(keys_to_delete)


class AttachmentToS3Handler(AttachmentHandler):
    """
    Save to a S3 or R2 compatible bucket and embed the assets in the transcript from there.
    Pass aiobotocore s3 client to the constructor. If none is pass, it will use CloudflareFactory to obtain one and
    Auto obtain the following from environment variables:

        - AWS_ENDPOINT_URL
        - AWS_ACCESS_KEY_ID
        - AWS_SECRET_ACCESS_KEY


    ## RAISES

        FileExistsError # if overwrite is False and file exists.

        ValueError # if file size exceeds 25MB.

        KeyError # if unable to determine if object exists.

        TypeError # if data is not bytes or io.BytesIO.

    """

    def __init__(
        self,
        aiobotocore_s3_client: Optional["S3Client"],
        bucket_name: str = "",
        key_prefix: str = "",
        compress_amount: Optional[int] = None,
        skip_files_which_are_too_large: bool = False,
    ):
        self.s3_client = aiobotocore_s3_client
        self.bucket_name = bucket_name
        self.key_prefix = key_prefix
        self.compress_amount = compress_amount
        self.skip_files_which_are_too_large = skip_files_which_are_too_large

        self.valid_image_exts = [
            ".png",
            ".jpg",
            ".jpeg",
            ".webp",
            ".bmp",
            ".tiff",
            ".gif",  # remove gif and add own compression handling...
        ]  # animated gifs will be converted to a single frame jpeg if even supported??
        self.uploaded_keys: List[str] = []

    async def process_asset(self, attachment: discord.Attachment) -> discord.Attachment:
        """Implement this to process the asset and return a url to the stored attachment.
        :param attachment: discord.Attachment
        :return: str
        """
        file_name = urllib.parse.quote_plus(
            f"{datetime.datetime.utcnow().timestamp()}_{attachment.filename}"
        )

        is_valid = any(
            attachment.filename.lower().endswith(ext) for ext in self.valid_image_exts
        )
        data_to_upload = None
        #

        try:
            session = await ClientSessionFactory.create_or_get_session()
            async with session.get(attachment.url) as res:
                if res.status != 200:
                    res.raise_for_status()
                data = io.BytesIO(await res.read())
                data.seek(0)
                if is_valid:
                    data_to_upload = self.compress_image(data)
                else:
                    data_to_upload = data
        except Exception as e:
            # fall back to not compressing..
            pass

        # upload to s3 / r2 bucket
        if data_to_upload is not None:
            self.key_prefix = self.key_prefix.removesuffix(
                "/"
            )  # ensure no trailing slash for combining.
            key = f"{self.key_prefix}/{file_name}" if self.key_prefix else file_name
            try:
                s3_manager = S3Manager(self.s3_client, self.bucket_name)
                content_type = self._get_content_type(file_name)
                await s3_manager.upload_file_data(
                    data=data_to_upload,
                    key_name=key,
                    overwrite=True,
                    content_type=content_type,
                    skip_files_which_are_too_large=self.skip_files_which_are_too_large,
                )
            except Exception as e:
                logger.error(
                    f"Error uploading to S3/R2: {e} - deleting the uploaded files from cloudflare R2/S3 bucket - Try again? "
                )
                await s3_manager.delete_files_in_list(self.uploaded_keys)
                raise e
        else:
            raise ValueError("Could not obtain data to upload to S3/R2 bucket.")

        file_url = f"/{self.key_prefix}/{file_name}"
        attachment.url = file_url
        attachment.proxy_url = file_url
        return attachment

    def _get_content_type(self, file_name: str) -> str:

        for ext in self.valid_image_exts:
            if file_name.lower().endswith(ext):
                content_type = f"image/{ext.lstrip('.')}"
                break
        if file_name.lower().endswith(".mp4"):
            content_type = "video/mp4"
        elif file_name.lower().endswith(".mp3"):
            content_type = "audio/mpeg"
        elif file_name.lower().endswith(".wav"):
            content_type = "audio/wav"
        elif file_name.lower().endswith(".ogg"):
            content_type = "audio/ogg"
        else:
            content_type = "html/text"
        return content_type

    def compress_image(self, data: io.BytesIO) -> io.BytesIO:
        image = Image.open(data)
        rgb_image = image.convert("RGB")
        compressed_data = io.BytesIO()
        rgb_image.save(compressed_data, format="JPEG", quality=self.compress_amount)
        compressed_data.seek(0)
        data_to_upload = compressed_data
        return data_to_upload


def _get_data_size(data: Union[io.BytesIO, bytes]) -> int:
    """Get the size of the data in bytes."""
    if isinstance(data, io.BytesIO):
        return data.getbuffer().nbytes
    elif isinstance(data, bytes):
        return len(data)
    else:
        raise TypeError(f"Unsupported data type: {type(data)}")
