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
import re
try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    Image = None
    PIL_AVAILABLE = False
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
IMG_DIR = os.path.join(os.path.dirname(__file__), "img")

def _normalize_image_name(name: str) -> str:
    if not name:
        return ""
    sanitized = re.sub(r"[^a-z0-9]+", "_", name.lower())
    return sanitized.strip("_")

def _load_image_index():
    images = {}
    if not os.path.isdir(IMG_DIR):
        return images
    for filename in os.listdir(IMG_DIR):
        if not filename.lower().endswith(".png"):
            continue
        key = _normalize_image_name(os.path.splitext(filename)[0])
        images[key] = os.path.join(IMG_DIR, filename)
    return images

CAR_IMAGE_INDEX = _load_image_index()

IMAGE_OVERRIDES = {
    "2914": [
        "416-m-a.png",
    ],
    "2916": [
        "416-m-a.png",
        "416-m-a.png",
    ],
    "2917": [
        "416-m-a.png",
    ],
    "2926": [
        "416-m-a.png",
        "416-m-a.png",
    ],
    "2934": [
        "BDbt-8007-3-b.png",
        "Bhv-2005.1-b.png",
        "Bhv-2005.1-b.png",
        "Bhv-2005.3-m-a.png",
        "418-a.png",
    ],
    "2944": [
        "BDbt-8007-3-b.png",
        "Bhv-2005.1-b.png",
        "Bhv-2005.1-b.png",
        "Bhv-2005.3-m-a.png",
        "418-a.png",
    ],
    "2949": [
        "418-a.png",
        "Bhv-2005.1-b.png",
        "Bhv-2005.1-b.png",
        "Bhv-2005.3-m-a.png",
        "BDbt-8007-3-a.png",
    ],
    "2969": [
        "418-a.png",
        "Bhv-2005.1-b.png",
        "Bhv-2005.3-m-a.png",
        "Bhv-2005.3-m-a.png",
        "BDbt-8007-3-a.png",
    ],
    "2979": [
        "416-m-a.png",
    ],
}


def _find_car_image_paths(vehicle_data: dict) -> list[str]:
    trip_short = str(vehicle_data.get("tripShortName") or "")
    vehicle_model = str(vehicle_data.get("vehicleModel") or "")
    uic_code = str(vehicle_data.get("uicCode") or "")
    vehicle_id = str(vehicle_data.get("vehicleId") or "")
    trip_number = "".join([c for c in trip_short if c.isdigit()])

    if trip_number in IMAGE_OVERRIDES:
        override_values = IMAGE_OVERRIDES[trip_number]
        if isinstance(override_values, str):
            override_values = [override_values]
        paths = []
        for filename in override_values:
            key = _normalize_image_name(os.path.splitext(filename)[0])
            path = CAR_IMAGE_INDEX.get(key)
            if path:
                paths.append(path)
        return paths

    candidates = [trip_short, trip_number, vehicle_model, uic_code, vehicle_id, "default"]
    for candidate in candidates:
        key = _normalize_image_name(candidate)
        if not key:
            continue
        if key in CAR_IMAGE_INDEX:
            return [CAR_IMAGE_INDEX[key]]
    return []


def _compose_trainset_image(image_paths: list[str], filename: str) -> discord.File:
    if not PIL_AVAILABLE:
        raise RuntimeError("Pillow is required to compose trainset images. Install Pillow in your container.")

    images = []
    for path in image_paths:
        try:
            img = Image.open(path).convert("RGBA")
            images.append(img)
        except Exception:
            continue

    if not images:
        raise ValueError("No valid images to compose")

    widths = [img.width for img in images]
    heights = [img.height for img in images]
    total_width = sum(widths)
    max_height = max(heights)

    combined = Image.new("RGBA", (total_width, max_height), (0, 0, 0, 0))
    x = 0
    for img in images:
        y = (max_height - img.height) // 2
        combined.paste(img, (x, y), mask=img)
        x += img.width

    output = io.BytesIO()
    combined.save(output, format="PNG")
    output.seek(0)
    return discord.File(output, filename=filename)


def _build_train_notification_embed(train_number: str, vehicle: dict, message_text: str) -> discord.Embed:
    next_stop = vehicle.get("nextStop", {}).get("stop", {}).get("name", "Ismeretlen")
    dest = vehicle.get("tripHeadsign") or "Ismeretlen"
    speed = round(vehicle.get("speed") or 0.0, 1)
    delay_sec = vehicle.get("nextStop", {}).get("arrivalDelay")
    delay_text = f"{int(delay_sec / 60)} perc" if delay_sec is not None else "—"

    embed = discord.Embed(
        title=message_text,
        color=0x00A0E3
    )
    embed.description = (
        f"Vonatszám: {train_number}\n"
        f"Cél: {dest}\n"
        f"Következő állomás: {next_stop}\n"
        f"Sebesség: {speed} km/h\n"
        f"Késés: {delay_text}"
    )
    return embed


async def _send_train_notification(channel, train_number: str, vehicle: dict, message_text: str):
    car_image_paths = _find_car_image_paths(vehicle)
    embed = _build_train_notification_embed(train_number, vehicle, message_text)

    if car_image_paths:
        filename = f"trainset_{train_number}.png"
        if PIL_AVAILABLE:
            try:
                image_file = _compose_trainset_image(car_image_paths, filename)
                embed.set_image(url=f"attachment://{filename}")
                await channel.send(embed=embed, file=image_file)
                return
            except Exception:
                pass

        files = [discord.File(path, filename=os.path.basename(path)) for path in car_image_paths]
        if files:
            embed.set_image(url=f"attachment://{files[0].filename}")
            await channel.send(embed=embed, files=files)
            return

    await channel.send(embed=embed)


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
    #Gyál > Kispest
    # {
    #     #6:39-7:03
    #     "channel_id": TRACKER_CHANNEL_ID,
    #     "train_number": "2969",
    #     "station_name": ["Gyál", "Gyál felső"],
    #     "weekdays": None,
    #     "last_next_stop": None,
    # },
    {
        #7:00-7:20
        "channel_id": TRACKER_CHANNEL_ID,
        "train_number": "2949",
        "station_name": ["Gyál", "Gyál felső"],
        "weekdays": ["tuesday", "wednesday", "thursday", "friday"],
        "last_next_stop": None,
    },
    # {
    #     #7:39-8:03
    #     "channel_id": TRACKER_CHANNEL_ID,
    #     "train_number": "2979",
    #     "station_name": ["Gyál", "Gyál felső"],
    #     "weekdays": None,
    #     "last_next_stop": None,
    # },
    # {
    #     #8:00-8:20
    #     "channel_id": TRACKER_CHANNEL_ID,
    #     "train_number": "2917",
    #     "station_name": ["Gyál", "Gyál felső"],
    #     "weekdays": ["monday", "wednesday"],
    #     "last_next_stop": None,
    # },

    #Kispest > Gyál
    {
        #10:37-10:57
        "channel_id": TRACKER_CHANNEL_ID,
        "train_number": "2914",
        "station_name": ["Kispest"],
        "weekdays": ["wednesday", "friday"],
        "last_next_stop": None,
    },
    # {
    #     #12:37-12:57
    #     "channel_id": TRACKER_CHANNEL_ID,
    #     "train_number": "2934",
    #     "station_name": ["Kispest"],
    #     "weekdays": None,
    #     "last_next_stop": None,
    # },
    # {
    #     #13:37-13:57
    #     "channel_id": TRACKER_CHANNEL_ID,
    #     "train_number": "2944",
    #     "station_name": ["Kispest"],
    #     "weekdays": ["wednesday", "friday"],
    #     "last_next_stop": None,
    # },
    # {
    #     #14:37-14:57
    #     "channel_id": TRACKER_CHANNEL_ID,
    #     "train_number": "2916",
    #     "station_name": ["Kispest"],
    #     "weekdays": ["tuesday", "wednesday", "thursday"],
    #     "last_next_stop": None,
    # },
    # {
    #     #15:37-15:57
    #     "channel_id": TRACKER_CHANNEL_ID,
    #     "train_number": "2926",
    #     "station_name": ["Kispest"],
    #     "weekdays": ["monday"],
    #     "last_next_stop": None,
    # }
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
    "ócsa",
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
            await _send_train_notification(channel, train_number, vehicle, message_text)

@bot.command(name="kocsik")
async def kocsik(ctx, vonatszam_keres: str = None):
    """Megjeleníti a tervezett kocsisort aktív vonatokhoz, ha a megfelelő PNG elérhető az img mappában."""
    all_vehicles = await fetch_mav_vehicles()

    matches = []
    if vonatszam_keres:
        vonatszam_keres = vonatszam_keres.strip()
        keresett_szam = normalize_trip_number(vonatszam_keres)
        for vid, data in all_vehicles.items():
            trip_short = str(data.get("tripShortName") or "")
            vonatszam = normalize_trip_number(trip_short)
            uic_szam = normalize_trip_number(data.get("uicCode") or "")
            if vonatszam == keresett_szam or str(vid) == vonatszam_keres or uic_szam == keresett_szam:
                matches.append({"vehicleId": vid, **data})
    else:
        for vid, data in all_vehicles.items():
            matches.append({"vehicleId": vid, **data})

    if not matches:
        if vonatszam_keres:
            fallback_car_paths = _find_car_image_paths({
                "tripShortName": vonatszam_keres,
                "vehicleModel": vonatszam_keres,
                "uicCode": vonatszam_keres,
                "vehicleId": vonatszam_keres,
            })
            if fallback_car_paths:
                planned_cars = [os.path.basename(p) for p in fallback_car_paths]
                filename = f"trainset_{vonatszam_keres}.png"
                embed = discord.Embed(
                    title=f"🚆 Tervezett kocsik: {vonatszam_keres}",
                    description=(
                        "Nincs aktív jármű a lekérdezés idején, de a megadott vonatszámhoz tartozó tervezett kocsikép elérhető.\n"
                        f"{ ' - '.join(planned_cars) }"
                    ),
                    color=0x00A0E3,
                )
                image_file = None
                if PIL_AVAILABLE:
                    try:
                        image_file = _compose_trainset_image(fallback_car_paths, filename)
                        embed.set_image(url=f"attachment://{filename}")
                        await ctx.send(embed=embed, file=image_file)
                        return
                    except Exception:
                        image_file = None

                files = [discord.File(path, filename=os.path.basename(path)) for path in fallback_car_paths]
                if files:
                    embed.set_image(url=f"attachment://{files[0].filename}")
                    await ctx.send(embed=embed, files=files)
                    return
        await ctx.send("Nincs aktív vonat, amelyhez kocsikép elérhető vagy nem található a megadott vonatszám.")
        return

    sent_any = False
    for v in matches:
        trip_short = str(v.get("tripShortName") or "")
        vonatszam = normalize_trip_number(trip_short) or str(v.get("vehicleId") or "?")
        cel = v.get("tripHeadsign") or "Ismeretlen"
        vehicle_model = v.get("vehicleModel") or "Ismeretlen"
        car_image_paths = _find_car_image_paths(v)
        planned_cars = [os.path.basename(p) for p in car_image_paths]

        if planned_cars:
            planned_text = " - ".join(planned_cars)
        else:
            planned_text = "Nincs megfelelő PNG az img mappában."

        description = (
            f"**Vonatszám:** {vonatszam}\n"
            f"**Cél:** {cel}\n"
            f"**Járműmodell:** {vehicle_model}\n"
            f"**Tervezett kocsisor:**\n{planned_text}"
        )

        embed = discord.Embed(
            title=f"🚆 Tervezett kocsik: {vonatszam}",
            description=description,
            color=0x00A0E3
        )

        if car_image_paths:
            filename = f"trainset_{vonatszam}.png"
            image_file = None
            if PIL_AVAILABLE:
                try:
                    image_file = _compose_trainset_image(car_image_paths, filename)
                except Exception:
                    image_file = None

            if image_file:
                embed.set_image(url=f"attachment://{filename}")
                await ctx.send(embed=embed, file=image_file)
            else:
                files = [discord.File(path, filename=os.path.basename(path)) for path in car_image_paths]
                if files:
                    embed.set_image(url=f"attachment://{files[0].filename}")
                    await ctx.send(embed=embed, files=files)
                else:
                    await ctx.send(embed=embed)
        else:
            await ctx.send(embed=embed)

        sent_any = True

    if not sent_any:
        await ctx.send("Nem sikerült elküldeni a tervezett kocsik embedet.")


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


@bot.command(name="helpvonatok")
async def helpvonatok(ctx):
    embed = discord.Embed(
        title="🚆 Figyelt vonatok menetrendje",
        color=0x00A0E3
    )

    embed.add_field(
        name="2969 - **inaktív**",
        value="Gyál ➜ Kispest\nIndul: 06:39\nÉrkezik: 07:03",
        inline=False
    )

    embed.add_field(
        name="2949",
        value="Gyál ➜ Kispest\nIndul: 07:00\nÉrkezik: 07:20",
        inline=False
    )

    embed.add_field(
        name="2979 - **inaktív**",
        value="Gyál ➜ Kispest\nIndul: 07:39\nÉrkezik: 08:03",
        inline=False
    )

    embed.add_field(
        name="2917",
        value="Gyál ➜ Kispest\nIndul: 08:00\nÉrkezik: 08:20",
        inline=False
    )

    embed.add_field(
        name="2934 - **inaktív**",
        value="Kispest ➜ Gyál\nIndul: 12:37\nÉrkezik: 12:57",
        inline=False
    )

    embed.add_field(
        name="2944",
        value="Kispest ➜ Gyál\nIndul: 13:37\nÉrkezik: 13:57",
        inline=False
    )

    embed.add_field(
        name="2916",
        value="Kispest ➜ Gyál\nIndul: 14:37\nÉrkezik: 14:57",
        inline=False
    )

    embed.add_field(
        name="2926",
        value="Kispest ➜ Gyál\nIndul: 15:37\nÉrkezik: 15:57",
        inline=False
    )

    await ctx.send(embed=embed)

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
