"""
Board list for the Futaba archiver.

Hosts MUST be lowercase: state keys and IA identifiers are case-sensitive.

Boards intentionally left out (not a `.../futaba.htm` catalog page, or no
concrete board path available): www.2chan.net/script/, jun.2chan.net/ascii/,
dec.2chan.net/up/ and /up2/ (uploader utility pages), the "2D Guro" and
"Erotic Games (Eroge)" entries (age-gate pages only), and "Figures"
(disabled on the site).

To run only some boards, set the BOARDS env var instead of editing this
file, e.g. BOARDS="may.2chan.net/b/,jun.2chan.net/jun/" (or BOARDS=all).
"""
from typing import NamedTuple


class Board(NamedTuple):
    host: str
    path: str


ALL_BOARDS = [
    Board("img.2chan.net", "/b/"),
    Board("may.2chan.net", "/b/"),
    Board("jun.2chan.net", "/jun/"),
    Board("dec.2chan.net", "/dec/"),
    Board("dec.2chan.net", "/84/"),  # Hololive
    Board("www.2chan.net", "/hinan/"),  # Refuge
    Board("zip.2chan.net", "/1/"),  # Baseball
    Board("zip.2chan.net", "/12/"),  # Soccer
    Board("may.2chan.net", "/25/"),  # Mahjong
    Board("may.2chan.net", "/26/"),  # Horse Racing
    Board("may.2chan.net", "/27/"),  # Cats
    Board("dat.2chan.net", "/d/"),  # Animals
    Board("zip.2chan.net", "/z/"),  # Plants
    Board("dat.2chan.net", "/w/"),  # Insects
    Board("dat.2chan.net", "/49/"),  # Aquariums
    Board("dec.2chan.net", "/62/"),  # Outdoors
    Board("dat.2chan.net", "/t/"),  # Cooking
    Board("dat.2chan.net", "/20/"),  # Sweets
    Board("dat.2chan.net", "/21/"),  # Ramen
    Board("dat.2chan.net", "/e/"),  # Vehicles
    Board("dat.2chan.net", "/j/"),  # Two-Wheelers
    Board("nov.2chan.net", "/37/"),  # Bicycles
    Board("dat.2chan.net", "/45/"),  # Cameras
    Board("dat.2chan.net", "/48/"),  # Consumer Electronics
    Board("dat.2chan.net", "/r/"),  # Trains
    Board("dat.2chan.net", "/img2/"),  # 2D
    Board("dec.2chan.net", "/58/"),  # 2D Alt Non-republishable
    Board("dec.2chan.net", "/59/"),  # 2D Alt Republishable
    Board("may.2chan.net", "/id/"),  # 2D with IDs
    Board("dat.2chan.net", "/23/"),  # Speed Grapher
    Board("dat.2chan.net", "/16/"),  # 2D Material/News
    Board("dat.2chan.net", "/43/"),  # 2D Industry
    Board("dec.2chan.net", "/74/"),  # Fate/Grand Order
    Board("dec.2chan.net", "/75/"),  # Idolm@ster
    Board("dec.2chan.net", "/86/"),  # ZOIDS
    Board("dec.2chan.net", "/78/"),  # Umehara General (FGC)
    Board("jun.2chan.net", "/31/"),  # Video Games
    Board("nov.2chan.net", "/28/"),  # Online Games (Netoge)
    Board("dec.2chan.net", "/56/"),  # Social Games (Soshage)
    Board("dec.2chan.net", "/60/"),  # KanColle
    Board("dec.2chan.net", "/69/"),  # Moai
    Board("dec.2chan.net", "/65/"),  # Touken Ranbu
    Board("dec.2chan.net", "/64/"),  # Divination
    Board("dec.2chan.net", "/66/"),  # Fashion
    Board("dec.2chan.net", "/67/"),  # Traveling
    Board("dec.2chan.net", "/68/"),  # Parenting
    Board("may.2chan.net", "/webm/"),  # WebM
    Board("dec.2chan.net", "/71/"),  # Yeah (Soudane)
    Board("dec.2chan.net", "/82/"),  # Nintendo
    Board("dec.2chan.net", "/61/"),  # Sony
    Board("dat.2chan.net", "/10/"),  # Net Characters
    Board("nov.2chan.net", "/34/"),  # Roleplaying
    Board("zip.2chan.net", "/11/"),  # OC Artwork
    Board("zip.2chan.net", "/14/"),  # OC Artwork Alt
    Board("zip.2chan.net", "/32/"),  # Crossdressing
    Board("zip.2chan.net", "/15/"),  # Bara
    Board("zip.2chan.net", "/7/"),  # Yuri
    Board("zip.2chan.net", "/8/"),  # Yaoi
    Board("zip.2chan.net", "/3/"),  # Self-Made PCs
    Board("cgi.2chan.net", "/g/"),  # Tokusatsu
    Board("zip.2chan.net", "/2/"),  # Mecha/Robot
    Board("dec.2chan.net", "/63/"),  # Movies
    Board("dat.2chan.net", "/44/"),  # Toys
    Board("dat.2chan.net", "/v/"),  # Model Kits
    Board("nov.2chan.net", "/y/"),  # Model Kits Alt (nov)
    Board("jun.2chan.net", "/47/"),  # Model Kits Alt (jun)
    Board("dec.2chan.net", "/73/"),  # VTubers
    Board("dec.2chan.net", "/81/"),  # Vocaloid
    Board("dat.2chan.net", "/x/"),  # 3DCG
    Board("dec.2chan.net", "/85/"),  # Artificial Intelligence
    Board("nov.2chan.net", "/35/"),  # Politics
    Board("nov.2chan.net", "/36/"),  # Economy
    Board("dec.2chan.net", "/79/"),  # Religion
    Board("dec.2chan.net", "/50/"),  # 3D Live Commentary
    Board("cgi.2chan.net", "/f/"),  # Military
    Board("may.2chan.net", "/39/"),  # Military Alt
    Board("cgi.2chan.net", "/m/"),  # Mathematics
    Board("cgi.2chan.net", "/i/"),  # Flash
    Board("cgi.2chan.net", "/k/"),  # Wallpapers
    Board("dat.2chan.net", "/l/"),  # 2D/Wallpapers
    Board("may.2chan.net", "/40/"),  # Touhou
    Board("zip.2chan.net", "/p/"),  # Oekaki
    Board("nov.2chan.net", "/q/"),  # Doodles
    Board("cgi.2chan.net", "/u/"),  # Doodles Alt
    Board("jun.2chan.net", "/oe/"),  # Oekaki MySQL
    Board("jun.2chan.net", "/72/"),  # Oekaki MySQL with IPs
    Board("zip.2chan.net", "/6/"),  # News Main
    Board("dec.2chan.net", "/76/"),  # Showa Era
    Board("dec.2chan.net", "/77/"),  # Heisei Era
    Board("dec.2chan.net", "/53/"),  # Power Generation
    Board("dec.2chan.net", "/52/"),  # Natural Disasters
    Board("dec.2chan.net", "/83/"),  # Corona
    Board("img.2chan.net", "/9/"),  # Idle Chat
    Board("dec.2chan.net", "/70/"),  # Board Proposals
    Board("ipv6.2chan.net", "/54/"),  # IPv6
    Board("may.2chan.net", "/layout/"),  # Layout
    Board("jun.2chan.net", "/junbi/"),  # Preparation (report)
    Board("cgi.2chan.net", "/o/"),
    Board("zip.2chan.net", "/5/"),
    Board("dat.2chan.net", "/b/"),
    Board("jun.2chan.net", "/51/"),
    Board("dec.2chan.net", "/55/"),
    Board("jun.2chan.net", "/30/"),  # (unlabeled in source board list)
    Board("dec.2chan.net", "/41/"),  # (unlabeled in source board list)
]
