"""Unit tests for the Spark transforms using a small local SparkSession.

    pytest 04-spark-data-lake      (needs Java 17+ and pyspark installed)
"""

import sys
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import etl  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield s
    s.stop()


def event(**kw):
    base = dict(artist="Band", auth="Logged In", firstName="Ann", gender="F", itemInSession=0,
                lastName="Lee", length=180.004, level="free", location="NYC", method="PUT",
                page="NextSong", registration=1.0, sessionId=7, song="World", status=200,
                ts=1_788_393_600_000, userAgent="UA", userId="5", _corrupt_record=None)
    base.update(kw)
    return base


@pytest.fixture
def songs_raw(spark):
    rows = [
        dict(num_songs=1, artist_id="AR1", artist_latitude=None, artist_longitude=None, artist_location="",
             artist_name="Band", song_id="SO1", title=" World ", duration=180.0, year=0),
        dict(num_songs=1, artist_id="AR1", artist_latitude=51.5, artist_longitude=-0.1, artist_location="London",
             artist_name="Band", song_id="SO2", title="Other", duration=99.0, year=2001),
    ]
    return spark.createDataFrame(rows, schema=etl.SONG_SCHEMA)


@pytest.fixture
def logs(spark):
    rows = [
        event(),
        event(),                                               # duplicate delivery
        event(itemInSession=1, ts=1_788_393_800_000, level="paid", song="Nope", artist="Nobody"),
        event(itemInSession=2, page="Home", song=None, artist=None, length=None),
        event(itemInSession=3, userId=""),                     # logged out
    ]
    return spark.createDataFrame(rows, schema=etl.LOG_SCHEMA)


def test_songs_and_artists(songs_raw):
    songs = {r.song_id: r for r in etl.songs_table(songs_raw).collect()}
    assert songs["SO1"].year is None and songs["SO1"].title == "World"
    artists = etl.artists_table(songs_raw).collect()
    assert len(artists) == 1 and artists[0].location == "London"


def test_plays_users_time(logs):
    plays = etl.song_plays(logs)
    assert plays.count() == 2
    users = etl.users_table(plays).collect()
    assert [(u.user_id, u.level) for u in users] == [(5, "paid")]
    t = etl.time_table(plays).orderBy("start_time").first()
    assert (t.year, t.month, t.day, t.hour, t.weekday) == (2026, 9, 3, 0, 3)  # a Thursday


def test_songplays_join_and_stable_id(songs_raw, logs):
    plays = etl.song_plays(logs)
    sp = etl.songplays_table(plays, etl.songs_table(songs_raw), etl.artists_table(songs_raw))
    rows = sorted(sp.collect(), key=lambda r: r.start_time)
    assert [r.song_id for r in rows] == ["SO1", None]
    assert len(rows[0].songplay_id) == 64
    again = etl.songplays_table(plays, etl.songs_table(songs_raw), etl.artists_table(songs_raw))
    assert sorted(r.songplay_id for r in again.collect()) == sorted(r.songplay_id for r in rows)
