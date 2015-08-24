drop table if exists tweets;
create external table tweets
  (id int, screen_name string, created_at string, text string, followers_count int)
  row format delimited fields terminated by ',' escaped by '\\'
  location '/demo/tweets';
