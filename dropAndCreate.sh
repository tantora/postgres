psql postgres -c "drop table contents;"
psql postgres -c "create table contents (item varchar(20), genre varchar(20)[], subgenre varchar(20)[], start timestamp, stop timestamp, primary key (item));"
psql postgres -c "\copy contents from 'data.txt' with null as '';"
psql postgres -c "drop table genres"
psql postgres -c "create table genres AS select unnest(genre) as genre , item from contents;"
psql postgres -c "create index genre_genre_index on genres  (genre);"

