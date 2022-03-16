import logging
logging.basicConfig(level = logging.DEBUG)
import sqlite3
from flask.cli import with_appcontext

# helper function that converts query result to json list, after cursor has executed a query
# this will not work for all endpoints direct, just the ones where you can translate
# a single query to the required json. 
def to_json(cursor):
    results = cursor.fetchall()
    headers = [d[0] for d in cursor.description]
    return [dict(zip(headers, row)) for row in results]


# Error class for when a key is not found
class KeyNotFound(Exception):
    def __init__(self, message=None):
        Exception.__init__(self)
        if message:
            self.message = message
        else:
            self.message = "Key/Id not found"

    def to_dict(self):
        rv = dict()
        rv['message'] = self.message
        return rv


# Error class for when request data is bad
class BadRequest(Exception):
    def __init__(self, message=None, error_code=400):
        Exception.__init__(self)
        if message:
            self.message = message
        else:
            self.message = "Bad Request"
        self.error_code = error_code

    def to_dict(self):
        rv = dict()
        rv['message'] = self.message
        return rv


"""
Wraps a single connection to the database with higher-level functionality.
Holds the DB connection
"""
class DB:
    def __init__(self, connection):
        self.conn = connection

    # Simple example of how to execute a query against the DB.
    # Again NEVER do this, you should only execute parameterized query
    # See https://docs.python.org/3/library/sqlite3.html#sqlite3.Cursor.execute
    # This is the qmark style:
    # cur.execute("insert into people values (?, ?)", (who, age))
    # And this is the named style:
    # cur.execute("select * from people where name_last=:who and age=:age", {"who": who, "age": age})
    def run_query(self, query):
        c = self.conn.cursor()
        c.execute(query)
        res = to_json(c)
        self.conn.commit()
        return res

    # Run script that drops and creates all tables
    def create_db(self, create_file):
        print("Running SQL script file %s" % create_file)
        with open(create_file, "r") as f:
            self.conn.executescript(f.read())
        return "{\"message\":\"created\"}"

    # Takes a list of 
    def get_artist_sets(self, artists):
        artist_set = set([])
        for a in artists:
            try:
                if a['artist_id'] < 1:
                    logging.error("Invalid artist ID")
                    raise BadRequest("Invalid artist ID")
                if a['artist_name'] == "":
                    logging.error("Invalid artist name")
                    raise BadRequest("Invalid artist name")
            except KeyError as e:
                logging.error("Missing artist ID or name")
                raise BadRequest("Missing artist ID or name")
            try:
                country = a['country']
            except KeyError as e:
                country = ""
            artist_set.add((a['artist_id'],str(a['artist_name']),country))
        return artist_set


    # Add an album to the DB
    # An album has details, a list of artists, and a list of songs
    # If the artist or songs already exist then they should not be created
    # The album should be associated with the artists.  The order does not matter
    # Songs sould be associated with the album, the order *does* matter and should be retained.
    def add_album(self, post_body):
        try:
            # logging.debug("Add Album with post %s" % post_body)
            album_id = post_body["album_id"]
            album_name = post_body["album_name"]
            release_year = post_body["release_year"]
            # An Artist is a dict of {"artist_id", "artist_name", "country" }
            # Arists is a list of artist [{"artist_id":12, "artist_name":"AA", "country":"XX"},{"arist_id": ...}]
            artists = post_body["artists"]
            # Songs is a list of { "song_id", "song_name", "length", "artist" }
            # Song Id an length are numbers, song_name is a string, artist is a list of artists (above)
            songs = post_body["songs"]
        except KeyError as e:
            raise BadRequest(message="Required attribute is missing")
        if isinstance(songs, list) is False or isinstance(artists, list) is False:
            logging.error("song_ids or artist_ids are not lists")
            raise BadRequest("song_ids or artist_ids are not lists")
        if len(songs) < 1 or len(artists) < 1:
            logging.error("song_ids or artist_ids are empty")
            raise BadRequest("song_ids or artist_ids are empty")

        # Album data construction
        if album_id < 1:
            logging.error("Invalid album ID")
            raise BadRequest("Invalid album ID")
        if release_year < 1900:
            logging.error("Invalid Release Year")
            raise BadRequest("Invalid Release Year")
        c = self.conn.cursor()
        # If album id exists,
        c.execute("SELECT EXISTS(SELECT album_id FROM Albums \
            WHERE album_id = ?)",(album_id,))
        if c.fetchone()[0]:
            # Raise error
            logging.error("This album ID already exists")
            raise BadRequest("album ID already exists")

        # Album artist data construction
        album_artist_set = self.get_artist_sets(artists)

        # Song data construction
        song_set = set([])
        song_artist_set = set([])
        song_album_set = set([])
        song_order = 1
        for s in songs:
            try:
                if s['song_id'] < 1:
                    logging.error("Invalid song ID")
                    raise BadRequest("Invalid song ID")
                if s['song_name'] == "":
                    logging.error("Invalid song name")
                    raise BadRequest("Invalid song name")
                if s['length'] < 1:
                    logging.error("Invalid song length")
                    raise BadRequest("Invalid song length")
                if len(s['artists']) < 1:
                    logging.error("Invalid song artists")
                    raise BadRequest("Invalid song artists")
            except KeyError as e:
                logging.error("Missing song information")
                raise BadRequest("Missing song information")
            
            for a in self.get_artist_sets(s['artists']):
                song_artist_set.add((s['song_id'],a))
            song_set.add((s['song_id'],s['song_name'],s['length']))
            song_album_set.add((s['song_id'],album_id,song_order))
            song_order += 1

        # Insert album
        c.execute("INSERT INTO Albums (album_id,album_name,release_year) \
            VALUES (?, ?, ?)",(album_id,album_name,release_year))
        
        for a in album_artist_set:
            # If artist does not exist
            c.execute("SELECT EXISTS(SELECT artist_id FROM Artists \
                WHERE artist_id = ?)",(a[0],))
            artist_inserted = c.fetchone()[0]
            if not artist_inserted:
                # Insert it
                c.execute("INSERT INTO Artists (artist_id,artist_name,country) \
                    VALUES (?,?,?)",a)

            # ARTIST_ALBUM RELATIONSHIP
            c.execute("INSERT or IGNORE INTO artist_album (artist_id,album_id) \
                VALUES (?,?)",(a[0],album_id))

        for s in song_set:
            # If song does not exist
            c.execute("SELECT EXISTS(SELECT song_id FROM Songs \
                WHERE song_id = ?)",(s[0],))
            song_inserted = c.fetchone()[0]
            if not song_inserted:
                # Insert it
                c.execute("INSERT INTO Songs (song_id,song_name,length) \
                    VALUES (?,?,?)",s)
        
        for sid,a in song_artist_set:
            # If artist does not exist
            c.execute("SELECT EXISTS(SELECT artist_id FROM Artists \
                WHERE artist_id = ?)",(a[0],))
            artist_inserted = c.fetchone()[0]
            if not artist_inserted:
                # Insert it
                c.execute("INSERT INTO Artists (artist_id,artist_name,country) \
                    VALUES (?,?,?)",a)
            
            # ARTIST_SONG RELATIONSHIP
            c.execute("INSERT or IGNORE INTO artist_song (artist_id,song_id) \
                VALUES (?,?)",(a[0],sid))

        for row in song_album_set:
            c.execute("INSERT INTO song_album (song_id,album_id,song_order) \
                VALUES (?,?,?)",row)

        self.conn.commit()
        # If your code successfully inserts the data
        return "{\"message\":\"album inserted\"}"



    """
    Returns a song's info
    raise KeyNotFound() if song_id is not found
    """
    def find_song(self, song_id):
        c = self.conn.cursor()
        c.execute("SELECT * FROM Songs WHERE song_id = ?",(song_id,))
        song_info = c.fetchall()
        if len(song_info) < 1:
            raise KeyNotFound()
        else:
            song_info = song_info[0]
        
        c.row_factory = lambda cursor, row: row[0]
        c.execute("SELECT DISTINCT artist_id FROM artist_song WHERE song_id = ?", (song_id,))
        artist_info = c.fetchall()
        c.execute("SELECT DISTINCT album_id FROM song_album WHERE song_id = ?", (song_id,))
        album_info = c.fetchall()
        self.conn.commit()
        return {
            'song_id': song_info[0],
            'song_name': song_info[1],
            'length': song_info[2],
            'artist_ids': artist_info,
            'album_ids': album_info
        }

    """
    Returns all an album's songs
    raise KeyNotFound() if album_id not found
    """
    def find_songs_by_album(self, album_id):
        c = self.conn.cursor()
        c.execute("SELECT EXISTS(SELECT album_id FROM Albums WHERE album_id = ?)",(album_id,))
        album_exists = c.fetchall()[0]
        if not album_exists:
            raise KeyNotFound()
        
        c.execute("SELECT song_id, song_name, length FROM song_album NATURAL JOIN Songs \
            WHERE album_id = ? ORDER BY song_order", (album_id,))
        res = to_json(c)
        c.row_factory = lambda cursor, row: row[0]
        for song in res:
            c.execute("SELECT DISTINCT artist_id FROM artist_song WHERE song_id = ?", (song['song_id'],))
            song['artist_ids'] = c.fetchall()
        self.conn.commit()
        return res

    """
    Returns all an artists' songs
    raise KeyNotFound() if artist_id is not found
    """
    def find_songs_by_artist(self, artist_id):
        c = self.conn.cursor()
        c.execute("SELECT EXISTS(SELECT artist_id FROM Artists WHERE artist_id = ?)",(artist_id,))
        artist_exists = c.fetchall()[0]
        if not artist_exists:
            raise KeyNotFound()
        
        c.execute("SELECT song_id, song_name, length FROM artist_song NATURAL JOIN Songs \
            WHERE artist_id = ? ORDER BY song_id", (artist_id,))
        res = to_json(c)
        c.row_factory = lambda cursor, row: row[0]
        for song in res:
            c.execute("SELECT DISTINCT artist_id FROM artist_song WHERE song_id = ? ORDER BY artist_id", (song['song_id'],))
            song['artist_ids'] = c.fetchall()
        self.conn.commit()
        return res
   
    """
    Returns a album's info
    raise KeyNotFound() if album_id is not found
    """
    def find_album(self, album_id):
        c = self.conn.cursor()
        c.execute("SELECT * FROM Albums WHERE album_id = ?",(album_id,))
        album_info = c.fetchall()
        if len(album_info) < 1:
            raise KeyNotFound()
        else:
            album_info = album_info[0]
        
        c.row_factory = lambda cursor, row: row[0]
        c.execute("SELECT DISTINCT artist_id FROM artist_album WHERE album_id = ?", (album_id,))
        artist_info = c.fetchall()
        c.execute("SELECT DISTINCT song_id FROM song_album WHERE album_id = ? ORDER BY song_order", (album_id,))
        song_info = c.fetchall()
        self.conn.commit()
        return {
            "album_id": album_info[0],
            "album_name": album_info[1],
            "release_year": album_info[2],
            "artist_ids": artist_info,
            "song_ids": song_info
        }

    """
    Returns a album's info
    raise KeyNotFound() if artist_id is not found 
    if artist exist, but there are no albums then return an empty result (from to_json)
    """
    def find_album_by_artist(self, artist_id):
        c = self.conn.cursor()
        c.execute("SELECT EXISTS(SELECT artist_id FROM Artists WHERE artist_id = ?)",(artist_id,))
        artist_exists = c.fetchall()[0]
        if not artist_exists:
            raise KeyNotFound()
        c.execute("SELECT album_id, album_name, release_year \
            FROM Albums NATURAL JOIN artist_album WHERE artist_id = ?",(artist_id,))
        res = to_json(c)
        self.conn.commit()
        return res

    """
    Returns a artist's info
    raise KeyNotFound() if artist_id is not found 
    """
    def find_artist(self, artist_id):
        c = self.conn.cursor()
        c.execute("SELECT EXISTS(SELECT artist_id FROM Artists WHERE artist_id = ?)",(artist_id,))
        artist_exists = c.fetchall()[0]
        if not artist_exists:
            raise KeyNotFound()
        c.execute("SELECT * FROM Artists WHERE artist_id = ?",(artist_id,))
        res = to_json(c)
        self.conn.commit()
        return res

    """
    Returns the average length of an artist's songs (artist_id, avg_length)
    raise KeyNotFound() if artist_id is not found 
    """
    def avg_song_length(self, artist_id):
        
        c = self.conn.cursor()
        c.execute("SELECT EXISTS(SELECT artist_id FROM Artists WHERE artist_id = ?)",(artist_id,))
        artist_exists = c.fetchall()[0]
        if not artist_exists:
            raise KeyNotFound()
        
        c.execute("SELECT ROUND(AVG(length),1) as avg_length, artist_id \
            FROM Songs NATURAL JOIN artist_song WHERE artist_id = ?",(artist_id,))
        res = to_json(c)
        self.conn.commit()
        return res


    """
    Returns top (n=num_artists) artists based on total length of songs
    """
    def top_length(self, num_artists):
        c = self.conn.cursor()
        c.execute("SELECT artist_id, SUM(length) as total_length \
            FROM Songs NATURAL JOIN artist_song GROUP BY artist_id \
                ORDER BY total_length DESC LIMIT ?",(num_artists,))
        res = to_json(c)
        self.conn.commit()
        return res
