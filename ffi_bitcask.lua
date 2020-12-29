local bit = require"bit"
local ffi = require"ffi"
local string = require"string"
local table = require"table"
local io = require"io"

local path = require"path"
local fs = require"path.fs"
local lhash = require"lhash"

local ffi_new, ffi_sizeof, ffi_copy, ffi_str = ffi.new, ffi.sizeof, ffi.copy, ffi.string
local str_fmt = string.format
local tinsert = table.insert

ffi.cdef [[
// record entry structure
typedef struct {
  uint32_t crc;         // CRC32
  uint32_t tm;          // create/modified timestamp
  uint32_t key_sz;      // key size
  uint32_t value_sz;    // value size
  // key content
  // value content
} record_t;

// hint entry structure
typedef struct {
  uint32_t tm;          // create/modified timestamp
  uint32_t key_sz;      // key size
  uint32_t value_sz;    // value size
  uint32_t value_pos;   // file offset
  // key content
} hint_t;
]]

-- hashmap
--   key -> {file_id, value_sz, value_pos, tm}

-- bucket-00
--   chunk-01
--     record
--     record
--     record
--     ...
--   chunk-02
--   chunk-03
--   chunk-04
--
-- bucket-01
--   ...
--
-- bucket-02
--   ...

local record_t_sz = ffi_sizeof("record_t")

local function create_record()
  return ffi_new("record_t")
end

local function read_record(fobj)
  local content = fobj:read(record_t_sz)
  if content == nil then
    return nil
  end
  local record = ffi_new("record_t")
  ffi_copy(record, content, record_t_sz)
  local key = fobj:read(record.key_sz)
  local value = fobj:read(record.value_sz)
  return record, key, value
end

local function write_record(fpath, record, key, value)
  local fobj = io.open(fpath, "ab+")
  if not fobj then
    return false
  end

  fobj:write(ffi_str(record, record_t_sz))
  fobj:write(key)
  if value then
    fobj:write(value)
  end
  fobj:close()
  return true
end

local function calc_record_crc(record)
  return lhash.ngx_crc(
    ffi_str(record,
      record_t_sz + record.key_sz + record.value_sz
    )
  )
end

local function gen_number_filename(s)
  return string.rep("0", 10 - s:len()) .. s
end

-- dir/c/f/0000000000.dat
local function get_chunk_filepath(self, chunk_file_id, bucket_id)
  local s = str_fmt("%02x", bucket_id)
  local a, b = s:sub(1, 1), s:sub(2, 2)
  local chunk_filename = gen_number_filename(tostring(chunk_file_id))
  return path.join(self.config.dir, a, b,
    str_fmt("%s.dat", chunk_filename)
  )
end

local function next_empty_chunk_file_id(bucket_info)
  local next_id = bucket_info.active_chunk_file_id + 1
  bucket_info.active_chunk_file_id = next_id
  return next_id
end

local function get_active_chunk_file_id(self, bucket_id)
  local bucket_info = self.buckets[bucket_id]
  local active_chunk_file_id = bucket_info.active_chunk_file_id
  local offset = 0
  while true do
    local file_sz = nil
    local fpath = get_chunk_filepath(self, active_chunk_file_id, bucket_id)
    if fs.exists(fpath) then
      file_sz = fs.fsize(fpath)
    end
    if file_sz ~= nil then
      if file_sz >= self.config.max_chunk_file_size then
        -- FIXME(xcc): check max_chunk_file_id
        active_chunk_file_id = next_empty_chunk_file_id(bucket_info)
      else
        offset = file_sz
        bucket_info.active_chunk_file_id = active_chunk_file_id
        break
      end
    else
      break
    end
  end

  return active_chunk_file_id, offset
end

local function get_bucket_id(self, key)
  local hash_code = self.hash_fn(key)
  return bit.band(hash_code, 0xFF)
end

local function create_bucket(self, bucket_id)
  self.buckets[bucket_id] = {
    active_chunk_file_id = 0,
    record_index_map = {}
  }

  local s = str_fmt("%02x", bucket_id)
  local a, b = s:sub(1, 1), s:sub(2, 2)
  fs.mkdir(path.join(self.config.dir, a))
  fs.mkdir(path.join(self.config.dir, a, b))
end

local function ensure_bucket(self, bucket_id)
  if not self.buckets[bucket_id] then
    create_bucket(self, bucket_id)
  end
end

local _M = {}
_M.__index = _M

-- GET
function _M:get(key)
  if type(key) ~= "string" or #key <= 0 then
    return nil
  end

  local bucket_id = get_bucket_id(self, key)
  ensure_bucket(self, bucket_id)
  local record_index_map = self.buckets[bucket_id].record_index_map
  local record_index = record_index_map[key]
  if record_index == nil then
    return nil
  end

  local fobj = io.open(get_chunk_filepath(self, record_index.file_id, bucket_id), "rb")
  if not fobj then
    return nil
  end
  fobj:seek("set", record_index.value_pos)
  local value = fobj:read(record_index.value_sz)
  fobj:close()

  if value and #value == record_index.value_sz then
    return value
  end

  return nil
end

-- PUT
function _M:put(key, value)
  local bucket_id = get_bucket_id(self, key)
  ensure_bucket(self, bucket_id)
  local record_index_map = self.buckets[bucket_id].record_index_map
  local record_index = record_index_map[key]
  if record_index ~= nil then
  end

  record = create_record()
  record.tm = os.time()
  record.key_sz = #key
  record.value_sz = #value
  record.crc = calc_record_crc(record)

  -- key -> file_id, value_sz, value_pos, tm
  local file_id, offset = get_active_chunk_file_id(self, bucket_id)
  record_index_map[key] = {
    file_id = file_id,
    value_sz = record.value_sz,
    value_pos = offset + record_t_sz + record.key_sz,
    tm = record.tm
  }
  return write_record(get_chunk_filepath(self, file_id, bucket_id), record, key, value)
end

-- DELETE
function _M:delete(key)
  if type(key) ~= "string" or #key <= 0 then
    return false
  end

  local bucket_id = get_bucket_id(self, key)
  ensure_bucket(self, bucket_id)
  local record_index_map = self.buckets[bucket_id].record_index_map
  local record_index = record_index_map[key]
  if record_index == nil then
    return false
  end

  record_index_map[key] = nil
  record = create_record()
  record.tm = os.time()
  record.key_sz = #key
  record.value_sz = 0
  record.crc = calc_record_crc(record)
  local file_id = get_active_chunk_file_id(self, bucket_id)
  return write_record(get_chunk_filepath(self, file_id, bucket_id), record, key, nil)
end

local function scan_data_dir(self, visit_fn)
  for fname1, ftype1 in fs.dir(self.config.dir) do
    if ftype1 == "dir" then
      for fname2, ftype2 in fs.dir(path.join(self.config.dir, fname1)) do
        if ftype2 == "dir" then
          local bucket_id = tonumber(fname1 .. fname2, 16)
          local bucket_dir = path.join(self.config.dir, fname1, fname2)
            for fname, ftype in fs.dir(bucket_dir) do
              if ftype == "file" then
                visit_fn(bucket_id, bucket_dir, fname)
              end
            end
        end
      end
    end
  end
end

function _M:list_bucket_ids()
  local tbl = {}
  for bucket_id, _ in pairs(self.buckets) do
    tinsert(tbl, bucket_id)
  end
  return tbl
end

function _M:list_keys(bucket_id)
  local bucket_info = self.buckets[bucket_id]
  if bucket_info == nil then
    return {}
  end

  local tbl = {}
  for key, _ in pairs(bucket_info.record_index_map) do
    tinsert(tbl, key)
  end
  return tbl
end

local function load_hints(self)
  scan_data_dir(self, function(bucket_id, bucket_dir, fname)
    if not self.buckets[bucket_id] then
      self.buckets[bucket_id] = {
        active_chunk_file_id = 0,
        record_index_map = {}
      }
    end

    local basename, ext = path.splitext(fname)
    local chunk_file_id = tonumber(basename)
    if ext == ".dat" then
      local bucket_info = self.buckets[bucket_id]
      if chunk_file_id > bucket_info.active_chunk_file_id then
        bucket_info.active_chunk_file_id = chunk_file_id
      end
      local record_index_map = bucket_info.record_index_map
      local fobj = io.open(path.join(bucket_dir, fname), "rb")
      while true do
        local offset = fobj:seek()
        local record, key = read_record(fobj)
        if record then
          if record.value_sz > 0 then
            record_index_map[key] = {
              file_id = chunk_file_id,
              value_sz = record.value_sz,
              value_pos = offset + record_t_sz + record.key_sz,
              tm = record.tm
            }
          else
            record_index_map[key] = nil
          end
        else
          fobj:close()
          break
        end
      end  -- end while
    end
  end)
end

local function open_db(config)
  if not config or type(config.dir) ~= "string" then
    return nil
  end

  fs.mkdir(config.dir)

  local obj = setmetatable({}, _M)
  obj.config = {
    dir = config.dir,
    max_chunk_file_size = config.max_chunk_file_size or (64 * 1024 * 1024)
  }
  obj.hash_fn = lhash.superfasthash
  obj.buckets = {}

  load_hints(obj)

  return obj
end

return {
  open = open_db
}
