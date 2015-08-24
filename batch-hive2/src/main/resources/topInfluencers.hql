insert overwrite directory '/demo/influencers'
 select distinct tweets.screen_name, tweets.followers_count from tweets
   order by tweets.followers_count desc
   limit 10;