drop table if exists Albums;
drop table if exists Artists;
drop table if exists Songs;
drop table if exists song_album;
drop table if exists artist_song;
drop table if exists artist_album;

create table Songs (
    song_id INTEGER CHECK (song_id >= 1) PRIMARY KEY,
    song_name VARCHAR(255) NOT NULL,
    length INTEGER CHECK (length >= 1) NOT NULL
);
create table Artists (
    artist_id INTEGER CHECK (artist_id >= 1) PRIMARY KEY,
    artist_name VARCHAR(255) NOT NULL,
    country VARCHAR(40)
);
create table Albums (
    album_id INTEGER CHECK (album_id >= 1) PRIMARY KEY,
    album_name VARCHAR(255) NOT NULL,
    release_year INTEGER CHECK (release_year >= 1900) NOT NULL
);
create table song_album (
    song_id INTEGER CHECK (song_id >= 1),
    album_id INTEGER CHECK (album_id >= 1),
    song_order INTEGER CHECK (song_order >= 1),
    PRIMARY KEY (song_id, album_id, song_order),
    FOREIGN KEY (song_id) REFERENCES Songs(song_id),
    FOREIGN KEY (album_id) REFERENCES Albums(album_id)
);
create table artist_song (
    artist_id INTEGER CHECK (artist_id >= 1),
    song_id INTEGER CHECK (song_id >= 1),
    PRIMARY KEY (artist_id, song_id),
    FOREIGN KEY (artist_id) REFERENCES Artists(artist_id),
    FOREIGN KEY (song_id) REFERENCES Songs(song_id)
);
create table artist_album (
    artist_id INTEGER CHECK (artist_id >= 1),
    album_id INTEGER CHECK (album_id >= 1),
    PRIMARY KEY (artist_id, album_id),
    FOREIGN KEY (artist_id) REFERENCES Artists(artist_id),
    FOREIGN KEY (album_id) REFERENCES Albums(album_id)
);