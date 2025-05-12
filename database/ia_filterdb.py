import logging
from struct import pack
import re
import base64
from pyrogram.file_id import FileId
from pymongo.errors import DuplicateKeyError
from umongo import Instance, Document, fields
from motor.motor_asyncio import AsyncIOMotorClient
from marshmallow.exceptions import ValidationError
from info import *
from utils import get_settings, save_group_settings


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

processed_movies = set()

MONGODB_SIZE_LIMIT = (512 * 1024 * 1024) - (80 * 1024 * 1024) 

client = AsyncIOMotorClient(DATABASE_URI)
db = client[DATABASE_NAME]
instance = Instance.from_db(db)

client2 = AsyncIOMotorClient(DATABASE_URI2)
db2 = client2[DATABASE_NAME]
instance2 = Instance.from_db(db2)


@instance.register
class Media(Document):
    file_id = fields.StrField(attribute='_id')
    file_ref = fields.StrField(allow_none=True)
    file_name = fields.StrField(required=True)
    file_size = fields.IntField(required=True)
    file_type = fields.StrField(allow_none=True)
    mime_type = fields.StrField(allow_none=True)
    caption = fields.StrField(allow_none=True)
    class Meta:
        indexes = ('$file_name', )
        collection_name = COLLECTION_NAME

@instance2.register
class Media2(Document):
    file_id = fields.StrField(attribute='_id')
    file_ref = fields.StrField(allow_none=True)
    file_name = fields.StrField(required=True)
    file_size = fields.IntField(required=True)
    file_type = fields.StrField(allow_none=True)
    mime_type = fields.StrField(allow_none=True)
    caption = fields.StrField(allow_none=True)
    class Meta:
        indexes = ('$file_name', )
        collection_name = COLLECTION_NAME

async def check_db_size(db):
    try:
        stats = await db.command("dbstats")
        return stats["dataSize"]
    except Exception as e:
        logger.error(f"Database size check error: {e}")
        return 0
         
async def save_file(bot, media):
    try:
        file_id, file_ref = unpack_new_file_id(media.file_id)
        file_name = re.sub(r"[^\w\s.-]", " ", str(media.file_name)).strip()       
        if await Media.count_documents({'file_id': file_id}, limit=1):
            print(f'{file_name} exists in primary DB')
            return False, 0
        target_db = Media
        if MULTIPLE_DB:
            primary_size = await check_db_size(db)
            if primary_size >= MONGODB_SIZE_LIMIT:
                print("Using secondary database")
                target_db = Media2
                if await Media2.count_documents({'file_id': file_id}, limit=1):
                    print(f'{file_name} exists in secondary DB')
                    return False, 0
        try:
            file = target_db(
                file_id=file_id,
                file_ref=file_ref,
                file_name=file_name,
                file_size=media.file_size,
                file_type=media.file_type,
                mime_type=media.mime_type,
                caption=media.caption.html if media.caption else None,
            )
            await file.commit()
            print(f'Saved to {target_db.__name__}: {file_name}')
            return True, 1
        except DuplicateKeyError:
            print(f'Duplicate file: {file_name}')
            return False, 0
    except Exception as e:
        print(f'Save error: {e}')
        return False, 2

async def get_search_results(chat_id, query, file_type=None, max_results=10, offset=0, filter=False):
    # Validate and normalize input parameters
    query = query.strip().lower() if query else ''
    max_results = max(1, min(int(max_results), 100))  # Ensure reasonable limits
    offset = max(0, int(offset))
    
    # Handle group settings
    if chat_id is not None:
        settings = await get_settings(int(chat_id))
        try:
            max_results = 10 if settings.get('max_btn') else min(int(MAX_B_TN), 100)
        except (KeyError, ValueError):
            await save_group_settings(int(chat_id), 'max_btn', False)
            settings = await get_settings(int(chat_id))
            max_results = 10 if settings.get('max_btn') else min(int(MAX_B_TN), 100)

    # Build more accurate search pattern
    if not query:
        raw_pattern = '.'
    else:
        # Escape special regex characters first
        escaped_query = re.escape(query)
        
        # Improved pattern matching:
        # 1. For single word queries: match whole words or parts with word boundaries
        # 2. For multi-word queries: match in order with flexible separators
        if ' ' not in query:
            raw_pattern = rf'(^|\b|\W){escaped_query}($|\b|\W)'
        else:
            words = escaped_query.split(r'\ ')
            raw_pattern = r'.*'.join([rf'({word})' for word in words])

    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except re.error:
        logger.error(f"Invalid regex pattern: {raw_pattern}")
        return [], '', 0

    # Build filter with improved accuracy
    filter_criteria = {}
    if USE_CAPTION_FILTER:
        filter_criteria['$or'] = [
            {'file_name': regex},
            {'caption': regex}
        ]
    else:
        filter_criteria['file_name'] = regex
    
    if file_type:
        filter_criteria['file_type'] = file_type.lower()

    # Get results with improved accuracy
    try:
        # Count total results
        total_results = await Media.count_documents(filter_criteria)
        if MULTIPLE_DB:
            total_results += await Media2.count_documents(filter_criteria)

        # Adjust max_results to be even if needed
        if max_results % 2 != 0:
            logger.debug(f"Adjusting odd max_results {max_results} to even number")
            max_results += 1

        # Get paginated results with proper sorting
        cursor1 = Media.find(filter_criteria).sort([('$natural', -1), ('_id', -1)]).skip(offset).limit(max_results)
        files1 = await cursor1.to_list(length=max_results)
        
        if MULTIPLE_DB:
            remaining_results = max(0, max_results - len(files1))
            if remaining_results > 0:
                cursor2 = Media2.find(filter_criteria).sort([('$natural', -1), ('_id', -1)]).skip(offset).limit(remaining_results)
                files2 = await cursor2.to_list(length=remaining_results)
                files = files1 + files2
            else:
                files = files1
        else:
            files = files1

        # Calculate next offset
        next_offset = offset + len(files)
        if next_offset >= total_results:
            next_offset = ''

        # Log search metrics for accuracy analysis
        logger.info(f"Search: '{query}' | Found: {len(files)}/{total_results} | Offset: {offset}")

        return files, next_offset, total_results

    except Exception as e:
        logger.error(f"Search error: {str(e)}", exc_info=True)
        return [], '', 0
async def get_bad_files(query, file_type=None, exact_match=False, page_size=50):
    """
    Search for files matching the query with improved accuracy.
    
    Args:
        query: Search string
        file_type: Optional file type filter ('audio', 'video', etc.)
        exact_match: If True, requires exact match of the whole string
        page_size: Number of results to return per page
        
    Returns:
        tuple: (matching_files, total_results)
    """
    query = query.strip()
    
    # Validate file_type if provided
    if file_type and file_type not in ['audio', 'video', 'document', 'photo']:
        return [], 0
    
    # Build regex pattern with improved accuracy
    if not query:
        raw_pattern = '.'
    elif exact_match:
        raw_pattern = f'^{re.escape(query)}$'
    elif ' ' not in query:
        raw_pattern = r'(^|\b|[\.\+\-_])' + re.escape(query) + r'($|\b|[\.\+\-_])'
    else:
        words = [re.escape(word) for word in query.split()]
        raw_pattern = r'[\s\.\+\-_()]*'.join(words)
    
    # Compile regex safely
    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except re.error:
        return [], 0
    
    # Build database filter
    filter = {'file_type': file_type} if file_type else {}
    if USE_CAPTION_FILTER:
        filter['$or'] = [{'file_name': regex}, {'caption': regex}]
    else:
        filter['file_name'] = regex
    
    # Query primary database
    cursor1 = Media.find(filter).sort('$natural', -1)
    files1 = await cursor1.to_list(length=page_size)
    total1 = await Media.count_documents(filter)
    
    # Query secondary database if enabled
    if MULTIPLE_DB:
        cursor2 = Media2.find(filter).sort('$natural', -1)
        files2 = await cursor2.to_list(length=page_size)
        total2 = await Media2.count_documents(filter)
        files = files1 + files2
        total = total1 + total2
    else:
        files = files1
        total = total1
    
    # Score and sort results by match quality
    def score_file(file):
        score = 0
        name = file.get('file_name', '')
        if regex.fullmatch(name):
            score += 2
        elif regex.search(name):
            score += 1
        if USE_CAPTION_FILTER and regex.search(file.get('caption', '')):
            score += 1
        return score
    
    files.sort(key=score_file, reverse=True)
    
    return files, total    
'''async def get_bad_files(query, file_type=None):
    query = query.strip()
    if not query:
        raw_pattern = '.'
    elif ' ' not in query:
        raw_pattern = r'(\b|[\.\+\-_])' + query + r'(\b|[\.\+\-_])'
    else:
        raw_pattern = query.replace(' ', r'.*[\s\.\+\-_()]')
    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except:
        return []
    if USE_CAPTION_FILTER:
        filter = {'$or': [{'file_name': regex}, {'caption': regex}]}
    else:
        filter = {'file_name': regex}
    if file_type:
        filter['file_type'] = file_type
    cursor1 = Media.find(filter).sort('$natural', -1)
    files1 = await cursor1.to_list(length=(await Media.count_documents(filter)))
    if MULTIPLE_DB:
        cursor2 = Media2.find(filter).sort('$natural', -1)
        files2 = await cursor2.to_list(length=(await Media2.count_documents(filter)))
        files = files1 + files2
    else:
        files = files1
    total_results = len(files)
    return files, total_results'''
    

async def get_file_details(query):
    filter = {'file_id': query}
    cursor = Media.find(filter)
    filedetails = await cursor.to_list(length=1)
    if not filedetails:
        cursor2 = Media2.find(filter)
        filedetails = await cursor2.to_list(length=1)
    return filedetails


def encode_file_id(s: bytes) -> str:
    r = b""
    n = 0
    for i in s + bytes([22]) + bytes([4]):
        if i == 0:
            n += 1
        else:
            if n:
                r += b"\x00" + bytes([n])
                n = 0
            r += bytes([i])
    return base64.urlsafe_b64encode(r).decode().rstrip("=")

def encode_file_ref(file_ref: bytes) -> str:
    return base64.urlsafe_b64encode(file_ref).decode().rstrip("=")

def unpack_new_file_id(new_file_id):
    decoded = FileId.decode(new_file_id)
    file_id = encode_file_id(
        pack(
            "<iiqq",
            int(decoded.file_type),
            decoded.dc_id,
            decoded.media_id,
            decoded.access_hash
        )
    )
    file_ref = encode_file_ref(decoded.file_reference)
    return file_id, file_ref
