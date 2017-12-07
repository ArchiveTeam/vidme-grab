dofile("urlcode.lua")
dofile("table_show.lua")
JSON = (loadfile "JSON.lua")()

local item_type = os.getenv('item_type')
local item_value = string.lower(os.getenv('item_value'))
local item_dir = os.getenv('item_dir')
local warc_file_base = os.getenv('warc_file_base')

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false

local identifiers = {}

local disco_users = {}
local disco_tags = {}

identifiers[item_value] = true

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

load_json_file = function(file)
  if file then
    return JSON:decode(file)
  else
    return nil
  end
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

allowed = function(url, parenturl)
  if string.match(url, "'+")
     or string.match(url, "[<>\\{}]")
     or string.match(url, "//$")
     or string.match(url, "locale%.lang")
     or string.match(url, "subscribe$")
     or not (string.match(url, "^https?://[^/]*vid%.me")
      or string.match(url, "^https?://[^/]*cloudfront%.net")) then
    return false
  end

  for s in string.gmatch(url, "([0-9a-zA-Z]+)") do
    if identifiers[s] == true then
      return true
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  if (downloaded[url] ~= true and addedtolist[url] ~= true)
     and (allowed(url, parent["url"]) or html == 0) then
    addedtolist[url] = true
    return true
  end

  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil

  downloaded[url] = true
  
  local function check(urla)
    local origurl = url
    local url = string.match(urla, "^([^#]+)")
    local url_ = string.gsub(url, "&amp;", "&")
    if (downloaded[url_] ~= true and addedtolist[url_] ~= true)
       and allowed(url_, origurl) then
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      check(string.match(url, "^(https?:)")..string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(string.match(url, "^(https?:)")..newurl)
    elseif string.match(newurl, "^\\/") then
      check(string.match(url, "^(https?://[^/]+)")..string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(string.match(url, "^(https?://[^/]+)")..newurl)
    end
  end

  local function checknewshorturl(newurl)
    if string.match(newurl, "^%?") then
      check(string.match(url, "^(https?://[^%?]+)")..newurl)
    elseif not (string.match(newurl, "^https?:\\?/\\?//?/?")
       or string.match(newurl, "^[/\\]")
       or string.match(newurl, "^[jJ]ava[sS]cript:")
       or string.match(newurl, "^[mM]ail[tT]o:")
       or string.match(newurl, "^vine:")
       or string.match(newurl, "^android%-app:")
       or string.match(newurl, "^%${")) then
      check(string.match(url, "^(https?://.+/)")..newurl)
    end
  end

  local function adduser(userurl)
    if userurl ~= nil then
      disco_users[string.match(userurl, "([^/]+)$")] = true
    end
  end

  local function addtags(comment)
    if comment ~= nil then
      for tag in string.gmatch(comment, "#([a-zA-Z][0-9a-zA-Z%-_%.]*)") do
        disco_tags[tag] = true
      end
    end
  end

  local function pagination(html, start, middle, end_, offset, limit, key)
    local data = load_json_file(html)
    if data ~= nil and data["status"] == true then
      offset = tonumber(offset)
      for k, v in pairs(data[key]) do
        offset = offset + 1
        adduser(v["user"]["full_url"])
        if key == "comments" then
          addtags(v["body"])
        end
      end
      check(start .. tostring(offset) .. middle .. limit .. end_)
    end
  end

  if string.match(url, "^https?://api%.vid%.me/") then
    check(string.match(url, "^(https?://)") .. "vid.me/api" .. string.match(url, "^https?://[^/]*(/.+)$"))
  end

  if string.match(url, "^https://vid%.me/api/") then
    check(string.match(url, "^(https?://)") .. "api.vid.me" .. string.match(url, "^https?://[^/]*/api(/.+)$"))
  end

  if allowed(url, nil) and not string.match(url, "^https?://[^/]*cloudfront%.net") then
    html = read_file(file)

    if string.match(url, "<title>403 Forbidden</title>") then
      abortgrab = true
      io.stdout:write("You are banned temporarily.\n")
      io.stdout:write("Sleeping 10 minutes and aborting...\n")
      os.execute("sleep 600")
    end

    if string.match(url, "^https?://vid%.me/e/[0-9a-zA-Z]+") then
      base = string.match(url, "^(https?://vid%.me/e/[0-9a-zA-Z]+)")
      check(base)
      check(base .. "?loop=1")
      check(base .. "?muted=1")
      check(base .. "?stats=1")
      check(base .. "?loop=1&muted=1")
      check(base .. "?muted=1&stats=1")
      check(base .. "?loop=1&stats=1")
      check(base .. "?loop=1&muted=1&stats=1")
      -- extra
      check(base .. "?muted=1&loop=1")
      check(base .. "?stats=1&loop=1")
      check(base .. "?stats=1&muted=1")
      check(base .. "?loop=1&stats=1&muted=1")
      check(base .. "?muted=1&loop=1&stats=1")
      check(base .. "?muted=1&stats=1&loop=1")
      check(base .. "?stats=1&loop=1&muted=1")
      check(base .. "?stats=1&muted=1&loop=1")
    end

    if string.match(url, "^https?://api%.vid%.me/video/[0-9]+$") then
      local data = load_json_file(html)
      if data["status"] == true and data["video"]["state"] ~= "deleted"
         and data["video"]["state"] ~= "suspended"
         and data["video"]["state"] ~= "user-disabled"
         and data["video"]["state"] ~= "reserved" then
        if data["video"]["state"] ~= "success" then
          abortgrab = true
        end
        identifiers[string.match(data["video"]["full_url"], "([0-9a-zA-Z]+)$")] = true
        if data["video"]["user"] ~= nil then
          adduser(data["video"]["user"]["full_url"])
        end
        addtags(data["video"]["description"])
        check(string.gsub(data["video"]["thumbnail_ai"], "{}", "120x"))
        check(string.gsub(data["video"]["thumbnail_ai"], "{}", "405x"))
        check(string.gsub(data["video"]["thumbnail_ai"], "{}", "600x"))
      end
    end

    if string.match(url, "^https?://api%.vid%.me/video/[0-9]+/likes%?offset=[0-9]+&limit=[0-9]+$") then
      local start, offset, middle, limit = string.match(url, "^(https?://api%.vid%.me/video/[0-9]+/likes%?offset=)([0-9]+)(&limit=)([0-9]+)$")
      pagination(html, start, middle, "", offset, limit, "votes")
    end

    if string.match(url, "^https?://api%.vid%.me/video/[0-9]+/comments%?offsetAtParentLevel=true&order=score&offset=[0-9]+&limit=[0-9]+$") then
      local start, offset, middle, limit = string.match(url, "^(https?://api%.vid%.me/video/[0-9]+/comments%?offsetAtParentLevel=true&order=score&offset=)([0-9]+)(&limit=)([0-9]+)$")
      pagination(html, start, middle, "", offset, limit, "comments")
    end

    for newurl in string.gmatch(html, '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
       checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      check(newurl)
    end
  end

  return urls
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  if (status_code >= 200 and status_code <= 399) then
    downloaded[url["url"]] = true
    downloaded[string.gsub(url["url"], "https?://", "http://")] = true
  end

  if abortgrab == true then
    io.stdout:write("ABORTING...\n")
    return wget.actions.ABORT
  end

  if (status_code == 400 or status_code == 404)
     and (string.match(url["url"], "^https?://api%.vid%.me/video/[0-9]+") or
          string.match(url["url"], "^https?://vid%.me/api/video/[0-9]+")) then
    return wget.actions.EXIT
  end
 
  if status_code >= 500 or
    (status_code >= 400 and status_code ~= 404 and status_code ~= 403) or
    status_code == 0 then
    io.stdout:write("Server returned "..http_stat.statcode.." ("..err.."). Sleeping.\n")
    io.stdout:flush()
    os.execute("sleep 1")
    tries = tries + 1
    if tries >= 5 then
      io.stdout:write("\nI give up...\n")
      io.stdout:flush()
      tries = 0
      if allowed(url["url"], nil) or status_code == 500 then
        return wget.actions.ABORT
      else
        return wget.actions.EXIT
      end
    else
      return wget.actions.CONTINUE
    end
  end

  tries = 0

  local sleep_time = 0

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local file = io.open(item_dir..'/'..warc_file_base..'_data.txt', 'w')
  for user, _ in pairs(disco_users) do
    file:write("user:" .. user .. "\n")
  end
  for tag, _ in pairs(disco_tags) do
    file:write("tag:" .. tag .. "\n")
  end
  file:close()
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab == true then
    return wget.exits.IO_FAIL
  end
  return exit_status
end