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
			raise ConnectionError("Could not obtain S3 client - Check your environment variables.")
        return cls._client

    @classmethod
    async def close(cls):
        """Close the singleton client."""
        if cls._context is not None:
            await cls._context.__aexit__(None, None, None)
            cls._client = None
            cls._context = None


class S3Manager:
    def __init__(self, aiobotocore_s3_client: Optional["S3Client"],  bucket: str = "bald-server"):
        self.bucket = bucket
        self.s3 = None

    async def get_all_keys(self) -> List[str]:
        client = await AsyncS3ClientManager.get_client()
        response = await client.list_objects_v2(Bucket=self.bucket)
        objects = []
        if "Contents" in response:
            for obj in response["Contents"]:
                if "Key" in obj:
                    objects.append(obj["Key"])
        return objects

    async def upload_file_data(
        self, data: Union[bytes, io.BytesIO], key_name: str, overwrite: bool = False
    ) -> None:
        """Upload a file-like object or bytes to Cloudflare R2/ s3 with ability to overwrite or not.

        RAISES:
        >>> FileExistsError # if overwrite is False and file exists.
        >>> ValueError # if file size exceeds 25MB.
        >>> KeyError # if unable to determine if object exists.
        >>> TypeError # if data is not bytes or io.BytesIO.
        """

        if not isinstance(data, (bytes, io.BytesIO)):
            raise TypeError("Data must be bytes or io.BytesIO")
        if isinstance(data, io.BytesIO):
            data.seek(0)
        if data.getbuffer().nbytes > 25_000_000:
            raise ValueError("File size exceeds 25MB limit. Cannot upload to R2")

        client = await AsyncS3ClientManager.get_client()

        try:
            if not overwrite:
                obj = await client.get_object(Bucket=self.bucket, Key=key_name)
                if obj is not None:
                    try:
                        status = obj["ResponseMetadata"]["HTTPStatusCode"]
                    except KeyError:
                        logger.warning(
                            "Could not determine if object exists, blocking upload."
                        )
                        return
                    if status == 200:
                        # Object exists, skip upload and raise error ?
                        raise FileExistsError(
                            f"Object with key {key_name} already exists in bucket {self.bucket}."
                        )  # raise bespoke error?

        except client.exceptions.NoSuchKey:
            pass  # we can proceed to upload as the object does not exist

        await client.put_object(Body=data, Bucket=self.bucket, Key=key_name)


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
    ):
        self.s3_client = aiobotocore_s3_client
        self.bucket_name = bucket_name
        self.key_prefix = key_prefix
        self.compress_amount = compress_amount

    def _get_data_size(self, data: Union[io.BytesIO, bytes]) -> int:
        """Get the size of the data in bytes."""
        if isinstance(data, io.BytesIO):
            return data.getbuffer().nbytes
        elif isinstance(data, bytes):
            return len(data)
        else:
            raise TypeError(f"Unsupported data type: {type(data)}")

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
        data_to_upload = None
        #
        
        if is_valid:
            try:
                session = await ClientSessionFactory.create_or_get_session()
                async with session.get(attachment.url) as res:
                    if res.status != 200:
                        res.raise_for_status()
                    data = io.BytesIO(await res.read())
                    data.seek(0)
                    data_to_upload = self.compress_image(data)
            except Exception as e:
                # fall back to not compressing..
                pass
		
		# upload to s3 / r2 bucket
		if data_to_upload is not None:
			key = f"{self.key_prefix}/{file_name}" if self.key_prefix else file_name
			s3_manager = S3Manager(self.s3_client, self.bucket_name)
			await s3_manager.upload_file_data(data=data_to_upload, key_name=key, overwrite=True)
		else:
			raise ValueError("Could not obtain data to upload to S3/R2 bucket.")

	
        file_url = f"/{self.key_prefix}/{file_name}"
        attachment.url = file_url
        attachment.proxy_url = file_url

    def compress_image(self, data: io.BytesIO) -> io.BytesIO:
        image = Image.open(data)
        rgb_image = image.convert("RGB")
        compressed_data = io.BytesIO()
        rgb_image.save(
                        compressed_data, format="JPEG", quality=self.compress_amount
                    ) 
        compressed_data.seek(0)
        data_to_upload = compressed_data
        return data_to_upload
        
        #
