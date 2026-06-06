from urllib import response
from datetime import datetime, UTC
import discord
from discord.ext import commands, tasks
import aiohttp
import os
import sys
import io
import csv
import zipfile
import asyncio
import requests
from datetime import UTC, datetime, timedelta
from collections import defaultdict
from google.transit import gtfs_realtime_pb2

# =======================
# BEÁLLÍTÁSOK
# =======================

TOKEN = os.getenv("TOKEN")

MAV_BASE = "https://mavplusz.hu"
JWT_URL = f"{MAV_BASE}/otp2-backend/otp/auth/get-jwt"
GRAPHQL_URL = f"{MAV_BASE}/otp2-backend/otp/routers/default/index/graphql"

REQ_TIMEOUT = aiohttp.ClientTimeout(total=25)
ACTIVE_STATUSES = {"IN_TRANSIT_TO", "STOPPED_AT", "IN_PROGRESS"}

GRAPHQL_QUERY = """
query VehiclePositions($swLat: Float!, $swLon: Float!, $neLat: Float!, $neLon: Float!) {
  vehiclePositions(swLat: $swLat, swLon: $swLon, neLat: $neLat, neLon: $neLon) {
    vehicleId
    lat
    lon
    heading
    vehicleModel
    label
    lastUpdated
    speed
    stopRelationship {
      status
      stop {
        gtfsId
        name
      }
      arrivalTime
      departureTime
    }
    trip {
      id
      gtfsId
      routeShortName
      tripHeadsign
      tripShortName
      route {
        mode
        shortName
        longName
        textColor
        color
      }
      pattern {
        id
      }
      serviceDate
    }
    nextStop {
      arrivalDelay
    }
    prevOrCurrentStop {
      scheduledArrival
      realtimeArrival
      arrivalDelay
      scheduledDeparture
      realtimeDeparture
      departureDelay
    }
  }
}
"""

COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": f"{MAV_BASE}/",
    "Origin": MAV_BASE,
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain, /",
}

import aiohttp
import asyncio

async def fetch_mav_vehicles():
    payload = {
        "query": GRAPHQL_QUERY,
        "variables": {
            "swLat": 47.2,
            "swLon": 18.7,
            "neLat": 47.75,
            "neLon": 19.6
        }
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Origin": MAV_BASE,
        "Referer": MAV_BASE + "/"
    }

    timeout = aiohttp.ClientTimeout(total=20)

    for attempt in range(3):  # retry 3x
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(GRAPHQL_URL, json=payload, headers=headers) as r:

                    print("STATUS:", r.status)

                    if r.status != 200:
                        print("Nem 200 válasz")
                        await asyncio.sleep(1)
                        continue

                    try:
                        resp_json = await r.json()
                    except Exception:
                        text = await r.text()
                        print("Nem JSON válasz:", text[:200])
                        return []

                    vehicles = resp_json.get("data", {}).get("vehiclePositions", [])

                    print(f"Lekért járművek: {len(vehicles)}")

                    return vehicles if vehicles else []

        except asyncio.TimeoutError:
            print(f"Timeout ({attempt+1}/3)")
        except aiohttp.ClientError as e:
            print(f"HTTP hiba ({attempt+1}/3):", e)
        except Exception as e:
            print(f"Egyéb hiba ({attempt+1}/3):", e)

        await asyncio.sleep(2)

    print("Fetch végleg elhasalt")
    return []

LOCK_FILE = "/tmp/discord_bot.lock"
DISCORD_LIMIT = 1900

if os.path.exists(LOCK_FILE):
    print("A bot már fut, kilépés.")
    sys.exit(0)

active_today_villamos = {}
active_today_combino = {}
active_today_caf5 = {}
active_today_caf9 = {}
active_today_tatra = {}
today_data = {}

# =======================
# GTFS / HELYKITÖLTŐK
# =======================

GTFS_PATH = ""
TXT_URL = ""

TRIPS_META = {}
STOPS = {}
TRIP_START = {}
TRIP_STOPS = defaultdict(list)
SERVICE_DATES = defaultdict(dict)
ROUTES = defaultdict(lambda: defaultdict(list))

# =======================
# DISCORD INIT
# =======================

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix=".", intents=intents)

# ─────────────────────────────────────────────
# HTTP / FEED SEGÉD
# ─────────────────────────────────────────────

UA_HEADERS = {
    "User-Agent": "BKK-DiscordBot/1.0 (+https://discord.com)"
}

def _http_get(url: str, timeout: int = 15) -> requests.Response:
    r = requests.get(url, headers=UA_HEADERS, timeout=timeout)
    if r.status_code != 200:
        snippet = (r.text or "")[:200].replace("\n", " ").replace("\r", " ")
        raise RuntimeError(f"HTTP {r.status_code} {r.reason}. Válasz eleje: {snippet}")
    return r

def fetch_pb_feed() -> gtfs_realtime_pb2.FeedMessage:
    r = _http_get(PB_URL)
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(r.content)
    except Exception as e:
        snippet = (r.content[:200] or b"").decode("utf-8", errors="replace").replace("\n", " ").replace("\r", " ")
        raise RuntimeError(f"PB parse hiba: {e}. Tartalom eleje: {snippet}")
    return feed

def fetch_txt_raw() -> str:
    r = _http_get(TXT_URL)
    return r.text or ""


# =======================
# SEGÉDFÜGGVÉNYEK
# =======================
def chunk_embeds(title_base, entries, color=0x003200, max_fields=20):
    embeds = []
    embed = discord.Embed(title=title_base, color=color)
    field_count = 0

    for reg, info in sorted(entries.items(), key=lambda x: x[0]):
        lat = info.get('lat')
        lon = info.get('lon')
        dest = info.get('dest', 'Ismeretlen')

        lat_str = f"{lat:.5f}" if lat is not None else "Ismeretlen"
        lon_str = f"{lon:.5f}" if lon is not None else "Ismeretlen"

        value = f"Cél: {dest}\nPozíció: {lat_str}, {lon_str}"

        if field_count >= max_fields:
            embeds.append(embed)
            embed = discord.Embed(title=f"{title_base} (folytatás)", color=color)
            field_count = 0

        embed.add_field(name=reg, value=value, inline=False)
        field_count += 1

    embeds.append(embed)
    return embeds

async def fetch_mav_vehicles():
    """Lekérdezi a MÁV járműveket a GraphQL API-ról Magyarország teljes területére."""
    query = """
    query VehiclePositions($swLat: Float!, $swLon: Float!, $neLat: Float!, $neLon: Float!) {
      vehiclePositions(swLat: $swLat, swLon: $swLon, neLat: $neLat, neLon: $neLon) {
        vehicleId
        lat
        lon
        heading
        vehicleModel
        label
        licensePlate
        uicCode
        lastUpdated
        speed
        stopRelationship {
          status
          stop {
            gtfsId
            name
          }
          arrivalTime
          departureTime
        }
        trip {
          id
          gtfsId
          routeShortName
          tripHeadsign
          tripShortName
          route {
            mode
            shortName
            longName
            textColor
            color
          }
          pattern {
            id
          }
          serviceDate
        }
        nextStop {
          arrivalDelay
          stop {
            name
          }
        }
        prevOrCurrentStop {
          scheduledArrival
          realtimeArrival
          arrivalDelay
          scheduledDeparture
          realtimeDeparture
          departureDelay
        }
      }
    }
    """
    variables = {
        "swLat": 45.7, "swLon": 16.0,  # Dél-nyugat
        "neLat": 48.6, "neLon": 22.9   # Észak-kelet
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(GRAPHQL_URL, json={"query": query, "variables": variables}) as resp:
            if resp.status != 200:
                return {}
            data = await resp.json()
            vehicles = data.get("data", {}).get("vehiclePositions", [])
            result = {}
            for v in vehicles:
                vid = v.get("vehicleId")
                if not vid:
                    continue
                # Teljes nextStop objektum default értékkel, ha nincs
                result[vid] = {
                    "lat": v.get("lat"),
                    "lon": v.get("lon"),
                    "vehicleModel": v.get("vehicleModel"),
                    "speed": v.get("speed"),
                    "uicCode": v.get("uicCode"),
                    "tripHeadsign": v.get("trip", {}).get("tripHeadsign"),
                    "tripShortName": v.get("trip", {}).get("tripShortName"),
                    "mode": v.get("trip", {}).get("route", {}).get("mode"),
                    "nextStop": v.get("nextStop") or {"arrivalDelay": None, "stop": {"name": "Ismeretlen"}}
                }
            return result
        
        
# =======================
# Logger loop
# =======================

active_mav_vehicles = {}
TRACKER_CHANNEL_ID = 1506284877006377074

tracked_vonatok = [
    {
        "channel_id": TRACKER_CHANNEL_ID,
        "train_number": "2949",
        "station_name": ["Gyál", "Gyál felső"],
        "weekdays": ["tuesday", "wednesday", "thursday", "friday"],
        "last_next_stop": None,
    },
    {
        "channel_id": TRACKER_CHANNEL_ID,
        "train_number": "2916",
        "station_name": ["Kispest"],
        "weekdays": ["tuesday", "wednesday", "thursday", "friday"],
        "last_next_stop": None,
    },
    {
        "channel_id": TRACKER_CHANNEL_ID,
        "train_number": "2917",
        "station_name": ["Gyál", "Gyál felső"],
        "weekdays": ["monday"],
        "last_next_stop": None,
    },
    {
        "channel_id": TRACKER_CHANNEL_ID,
        "train_number": "2926",
        "station_name": ["Kispest"],
        "weekdays": ["monday"],
        "last_next_stop": None,
    }
]
DEFAULT_LATE_THRESHOLD = 1 * 60  # 1 perc másodpercben

def normalize_trip_number(trip_short):
    return "".join([c for c in str(trip_short or "") if c.isdigit()])

TRACKED_STOPS = {
    "kőbánya-kispest",
    "kispest",
    "pestszentimre felső",
    "pestszentimre",
    "gyál felső",
    "gyál",
    "felsőpakony",
}

@tasks.loop(seconds=30)
async def logger_loop_mav():
    """Frissíti a MÁV járművek állapotát Magyarország teljes területére vonatkozóan."""
    try:
        vehicles = await fetch_mav_vehicles()  # a korábbi fetch függvény
    except Exception as e:
        print(f"Hiba a járművek lekérésekor: {e}")
        return

    now = datetime.now()
    active_mav_vehicles.clear()

    for vid, v in vehicles.items():
        lat = v.get("lat")
        lon = v.get("lon")
        dest = v.get("tripHeadsign") or "Ismeretlen"

        active_mav_vehicles[vid] = {
            "lat": lat,
            "lon": lon,
            "dest": dest,
            "vehicleModel": v.get("vehicleModel"),
            "speed": v.get("speed"),
            "uicCode": v.get("uicCode"),
            "tripShortName": v.get("tripShortName"),
            "mode": v.get("mode"),
            "nextStop": v.get("nextStop"),
            "last_seen": now
        }

# =======================
# PARANCSOK - Egyébbek
# =======================
        
@tasks.loop(minutes=5)
async def vonat_watch_loop():
    """Figyeli a regisztrált vonatokat, és küld értesítést késés vagy következő megálló változás esetén."""
    if not tracked_vonatok:
        return

    all_vehicles = await fetch_mav_vehicles()
    for tracker in tracked_vonatok:
        channel = bot.get_channel(tracker["channel_id"])
        if channel is None:
            continue

        train_number = tracker["train_number"]
        station_name = tracker.get("station_name")
        tracker_weekdays = tracker.get("weekdays")
        today = datetime.now().strftime("%A").lower()
        if tracker_weekdays and today not in tracker_weekdays:
            continue

        station_name_norm = None
        if station_name is not None:
            if isinstance(station_name, list):
                station_name_norm = [name.lower() for name in station_name]
            else:
                station_name_norm = [station_name.lower()]

        matches = []

        for vid, data in all_vehicles.items():
            if normalize_trip_number(data.get("tripShortName")) == train_number:
                matches.append({"vehicleId": vid, **data})

        if not matches:
            continue

        # Ha több találat van, az elsőt használjuk.
        vehicle = matches[0]
        next_stop = vehicle.get("nextStop", {}).get("stop", {}).get("name", "Ismeretlen")
        delay_sec = vehicle.get("nextStop", {}).get("arrivalDelay")
        is_late = delay_sec is not None and delay_sec > DEFAULT_LATE_THRESHOLD
        next_stop_norm = next_stop.lower()
        station_match = False
        if station_name_norm is not None:
            station_match = any(name in next_stop_norm for name in station_name_norm)

        if not any(stop in next_stop_norm for stop in TRACKED_STOPS):
            continue

        should_send = False
        message_text = None

        if is_late:
            delay_min = int(delay_sec / 60)
            message_text = f"A vonat késik {delay_min} percet ({train_number})"
            should_send = True
        elif station_match and tracker.get("last_next_stop") != next_stop:
            message_text = f"A következő megálló {next_stop} ({train_number})"
            should_send = True

        tracker["last_next_stop"] = next_stop

        if should_send and message_text:
            await channel.send(message_text)

@bot.command()
async def vonat(ctx, vonatszam_keres: str, *, station_name: str = None):
    """Megkeresi a megadott vonatszámú járművet és kiírja az adatait.
    Ha megadsz állomásnevet, figyelést is indít arra a vonatra."""
    if station_name:
        station_name = station_name.strip()
        if not station_name:
            await ctx.send("❌ Add meg a figyelni kívánt állomást is.")
            return

        tracked_vonatok.append({
            "channel_id": ctx.channel.id,
            "train_number": vonatszam_keres,
            "station_name": station_name,
            "last_next_stop": None,
        })
        await ctx.send(
            f"📌 Figyelés elindítva: {vonatszam_keres} vonat követése. Értesítés 5 percenként, ha >10 perc késés vagy ha a következő megálló {station_name} lesz."
        )
        return

    all_vehicles = await fetch_mav_vehicles()  # dict {vehicleId: adatok}

    matches = []
    for vid, data in all_vehicles.items():
        trip_short = str(data.get("tripShortName") or "")
        vonatszam = "".join([c for c in trip_short if c.isdigit()])
        if vonatszam == vonatszam_keres:
            matches.append({"vehicleId": vid, **data})

    if not matches:
        await ctx.send(f"Nincs aktív jármű a(z) {vonatszam_keres} vonatszámmal.")
        return

    MAX_CHARS = 4000
    description = ""
    embeds = []

    for v in matches:
        uic = str(v.get("uicCode") or "Ismeretlen")
        # Pályaszám kiírás
        if len(uic) >= 11:
            payaszam = f"{uic[5:8]} {uic[8:11]}" + (f"-{uic[11]}" if len(uic) > 11 else "")
        else:
            payaszam = uic

        cel = v.get("tripHeadsign") or "Ismeretlen"
        next_stop = v.get("nextStop", {}).get("stop", {}).get("name", "Ismeretlen")
        speed = round(v.get("speed") or 0.0, 1)
        delay_sec = v.get("nextStop", {}).get("arrivalDelay")
        delay_min = f"{int(delay_sec / 60)} perc" if delay_sec is not None else "—"

        entry = (
            f"**{payaszam}**\n"
            f"UIC: {uic}\n"
            f"Célállomás: {cel}\n"
            f"Következő állomás: {next_stop}\n"
            f"Sebesség: {speed} km/h\n"
            f"Késés: {delay_min}\n\n"
        )

        if len(description) + len(entry) > MAX_CHARS:
            embed = discord.Embed(
                title=f"🚆 Vonat {vonatszam_keres} adatai (folytatás)",
                description=description,
                color=0x00A0E3
            )
            embeds.append(embed)
            description = entry
        else:
            description += entry

    if description:
        title = f"🚆 Vonat {vonatszam_keres} adatai" if not embeds else f"🚆 Vonat {vonatszam_keres} adatai (folytatás)"
        embed = discord.Embed(
            title=title,
            description=description,
            color=0x00A0E3
        )
        embeds.append(embed)

    for e in embeds:
        await ctx.send(embed=e)

# =======================
# START
# =======================

@bot.event
async def on_ready():
    print(f"Bejelentkezve mint {bot.user}")
    if not vonat_watch_loop.is_running():
        vonat_watch_loop.start()

try:
    bot.run(TOKEN)
finally:
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
    except Exception:
        pass
