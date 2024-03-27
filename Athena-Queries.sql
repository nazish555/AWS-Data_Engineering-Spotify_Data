
---------------Count of tracks against each artist-------

SELECT artist_id, COUNT(track_id) AS num_tracks
FROM tracks
GROUP BY artist_id
ORDER BY num_tracks DESC;


---------------List top tracks based on popularity---------

SELECT track_name, popularity
FROM tracks
ORDER BY popularity DESC
LIMIT 10;

-----------------Calculate the total number of streams per album------

SELECT album_id, album_name, SUM(streams) AS total_streams
FROM albums
GROUP BY album_id, album_name
ORDER BY total_streams DESC;


--------------Determine the average popularity of tracks by year-------

SELECT EXTRACT(YEAR FROM release_date) AS release_year,
AVG(popularity) AS avg_popularity
FROM tracks
GROUP BY EXTRACT(YEAR FROM release_date)
ORDER BY release_year;


-------Find artists with the highest total streams--------

SELECT artist_id, artist_name, SUM(streams) AS total_streams
FROM tracks
GROUP BY artist_id, artist_name
ORDER BY total_streams DESC
LIMIT 10;



