library("DBI")
library("RPostgres")
library("tidyjson")
library("jsonlite")
library("stringr")

options(error=traceback)

print("Start analyzing twitter stream")
print (Sys.time())

# R write top tweets to DB
db_settings = Sys.getenv(c('R_DB_NAME', 'R_DB_HOST', 'R_DB_PORT', 'R_DB_USER', 'R_DB_PASSWORD'))

con <- dbConnect(RPostgres::Postgres(), 
	dbname = as.character(db_settings['R_DB_NAME']), 
	host = as.character(db_settings['R_DB_HOST']), 
	port = as.numeric(db_settings['R_DB_PORT']), 
	user = as.character(db_settings['R_DB_USER']), 
	password = as.character(db_settings['R_DB_PASSWORD']))

init <- function()
{
	daily_list <<- list(list(), list(), list(), list(), list(), list(), list(), list())
	names(daily_list) <- c("retweets", "tweets", "users", "places", "mentions", 
		"direct_messages", "collections", "lists")
		
	# list of tracked attributes 
	# tweets
	daily_list$tweets$list_created_at <<- list()
	daily_list$tweets$list_retweet_count <<- list()
	daily_list$tweets$list_favorite_count <<- list()
	daily_list$tweets$list_favorited <<- list()
	daily_list$tweets$list_retweeted <<- list()
	daily_list$tweets$list_filter_level <<- list()
	daily_list$tweets$list_lang <<- list()
	daily_list$tweets$list_id_str <<- list()
	daily_list$tweets$list_text <<- list()
	daily_list$tweets$list_truncated <<- list()
	daily_list$tweets$list_user_id_str <<- list()
	# retweets
	daily_list$retweets$list_created_at <<- list()
	daily_list$retweets$list_retweet_count <<- list()
	daily_list$retweets$list_favorite_count <<- list()
	daily_list$retweets$list_favorited <<- list()
	daily_list$retweets$list_retweeted <<- list()
	daily_list$retweets$list_filter_level <<- list()
	daily_list$retweets$list_lang <<- list()
	daily_list$retweets$list_id_str <<- list()
	daily_list$retweets$list_text <<- list()
	daily_list$retweets$list_truncated <<- list()
	daily_list$retweets$list_user_id_str <<- list()
	daily_list$retweets$list_retweeted_status_id_str <<- list()	
	# retweeted
	daily_list$retweeted$list_created_at <<- list()
	daily_list$retweeted$list_retweet_count <<- list()
	daily_list$retweeted$list_favorite_count <<- list()
	daily_list$retweeted$list_favorited <<- list()
	daily_list$retweeted$list_retweeted <<- list()
	daily_list$retweeted$list_filter_level <<- list()
	daily_list$retweeted$list_lang <<- list()
	daily_list$retweeted$list_id_str <<- list()
	daily_list$retweeted$list_text <<- list()
	daily_list$retweeted$list_truncated <<- list()
	daily_list$retweeted$list_user_id_str <<- list()
}

parse_daily_tweets <- function(i) {
	#print("before fromJSON")
	#print(i)
	status_attrs_list = fromJSON(stream[i,1])
	#print(status_attrs_list)

	create_at = as.POSIXct(strptime(status_attrs_list$created_at, "%a %b %d %H:%M:%S %z %Y"))
	if (as.numeric(Sys.time() - create_at, units = "days") > 1) { # tweet is older than a day
		#print(status_attrs_list$created_at)
		#print(as.numeric(Sys.time() - create_at, units = "days"))
		return()
	}
	
	#print(as.numeric(Sys.time() - create_at, units = "hours"))
	#print(status_attrs_list$created_at)
	if ("retweeted_status" %in% names(status_attrs_list) == FALSE) {
		num_tweets <<- num_tweets + 1
		daily_list$tweets$list_created_at[num_tweets] <<- status_attrs_list$created_at
		daily_list$tweets$list_retweet_count[num_tweets] <<- status_attrs_list$retweet_count
		daily_list$tweets$list_favorite_count[num_tweets] <<- status_attrs_list$favorite_count
		daily_list$tweets$list_favorited[num_tweets] <<- status_attrs_list$favorited
		daily_list$tweets$list_retweeted[num_tweets] <<- status_attrs_list$retweeted
		daily_list$tweets$list_filter_level[num_tweets] <<- status_attrs_list$filter_level
		daily_list$tweets$list_lang[num_tweets] <<- status_attrs_list$lang
		daily_list$tweets$list_id_str[num_tweets] <<- status_attrs_list$id_str
		daily_list$tweets$list_text[num_tweets] <<- status_attrs_list$text
		daily_list$tweets$list_source[num_tweets] <<- status_attrs_list$source
		daily_list$tweets$list_truncated[num_tweets] <<- status_attrs_list$truncated
	} else {
		parse_retweet(i, status_attrs_list)
	}

	# Add to attribute vectors
}

parse_retweet <- function(i, status_attrs_list) {
	num_retweets <<- num_retweets + 1
	daily_list$retweets$list_created_at[num_retweets] <<- status_attrs_list$created_at
	daily_list$retweets$list_retweet_count[num_retweets] <<- status_attrs_list$retweet_count
	daily_list$retweets$list_favorite_count[num_retweets] <<- status_attrs_list$favorite_count
	daily_list$retweets$list_favorited[num_retweets] <<- status_attrs_list$favorited
	daily_list$retweets$list_retweeted[num_retweets] <<- status_attrs_list$retweeted
	daily_list$retweets$list_filter_level[num_retweets] <<- status_attrs_list$filter_level
	daily_list$retweets$list_lang[num_retweets] <<- status_attrs_list$lang
	daily_list$retweets$list_id_str[num_retweets] <<- status_attrs_list$id_str
	daily_list$retweets$list_text[num_retweets] <<- status_attrs_list$text
	daily_list$retweets$list_source[num_retweets] <<- status_attrs_list$source
	daily_list$retweets$list_truncated[num_retweets] <<- status_attrs_list$truncated
	daily_list$retweets$list_retweeted_status_id_str[num_retweets] <<- status_attrs_list$retweeted_status$id_str
	
	# preserve the original tweet
	daily_list$retweeted$list_created_at[num_retweets] <<- status_attrs_list$retweeted_status$created_at
	daily_list$retweeted$list_retweet_count[num_retweets] <<- status_attrs_list$retweeted_status$retweet_count
	daily_list$retweeted$list_favorite_count[num_retweets] <<- status_attrs_list$retweeted_status$favorite_count
	daily_list$retweeted$list_favorited[num_retweets] <<- status_attrs_list$retweeted_status$favorited
	daily_list$retweeted$list_retweeted[num_retweets] <<- status_attrs_list$retweeted_status$retweeted
	daily_list$retweeted$list_filter_level[num_retweets] <<- status_attrs_list$retweeted_status$filter_level
	daily_list$retweeted$list_lang[num_retweets] <<- status_attrs_list$retweeted_status$lang
	daily_list$retweeted$list_id_str[num_retweets] <<- status_attrs_list$retweeted_status$id_str
	daily_list$retweeted$list_text[num_retweets] <<- status_attrs_list$retweeted_status$text
	daily_list$retweeted$list_source[num_retweets] <<- status_attrs_list$retweeted_status$source
	daily_list$retweeted$list_truncated[num_retweets] <<- status_attrs_list$retweeted_status$truncated
}

parse_daily_stream <- function() {
	stream <<- dbGetQuery(con, "SELECT status FROM daily_stream")
	i = 1
	repeat {
		if (i > nrow(stream)) {
		#if (i > 3) {
			break
		}
		
		parse_daily_tweets(i)
		i <- i + 1
	}	
}

create_data_frames <- function() {
	tweet_df <<- data.frame(
		create_at = unlist(daily_list$tweets$list_created_at),
		retweet_count = unlist(daily_list$tweets$list_retweet_count),
		favorite_count = unlist(daily_list$tweets$list_favorite_count),
		favorited = unlist(daily_list$tweets$list_favorited),
		retweeted = unlist(daily_list$tweets$list_retweeted),
		filter_level = unlist(daily_list$tweets$list_filter_level),
		lang = unlist(daily_list$tweets$list_lang),
		id_str = unlist(daily_list$tweets$list_id_str),
		text = unlist(daily_list$tweets$list_text),
		source = unlist(daily_list$tweets$list_source),
		truncated = unlist(daily_list$tweets$list_truncated),
		stringsAsFactors = FALSE	
		)	
		
	retweet_df <<- data.frame(
		create_at = unlist(daily_list$retweets$list_created_at),
		retweet_count = unlist(daily_list$retweets$list_retweet_count),
		favorite_count = unlist(daily_list$retweets$list_favorite_count),
		favorited = unlist(daily_list$retweets$list_favorited),
		retweeted = unlist(daily_list$retweets$list_retweeted),
		filter_level = unlist(daily_list$retweets$list_filter_level),
		lang = unlist(daily_list$retweets$list_lang),
		id_str = unlist(daily_list$retweets$list_id_str),
		text = unlist(daily_list$retweets$list_text),
  		source = unlist(daily_list$retweets$list_source),
  		truncated = unlist(daily_list$retweets$list_truncated),
  		retweeted_status_id_str = unlist(daily_list$retweets$list_retweeted_status_id_str),
		stringsAsFactors = FALSE	
		)	
	
	retweeted_df <<- data.frame(
		id = unlist(daily_list$retweeted$list_id_str),
		create_at = unlist(daily_list$retweeted$list_created_at),
		retweet_count = unlist(daily_list$retweeted$list_retweet_count),
		favorite_count = unlist(daily_list$retweeted$list_favorite_count),
		favorited = unlist(daily_list$retweeted$list_favorited),
		retweeted = unlist(daily_list$retweeted$list_retweeted),
		filter_level = unlist(daily_list$retweeted$list_filter_level),
		lang = unlist(daily_list$retweeted$list_lang),
		#timestamp_ms = unlist(daily_list$retweeted$list_timestamp_ms),
		text = unlist(daily_list$retweeted$list_text),
  		source = unlist(daily_list$retweeted$list_source),
  		truncated = unlist(daily_list$retweeted$list_truncated),
		stringsAsFactors = FALSE	
		)	
}

db_write_retweeted <- function() {	
	dbSendQuery(con, "DROP TABLE IF EXISTS retweeted")
	dbSendQuery(con, "CREATE TABLE retweeted(id VARCHAR(20) PRIMARY KEY, create_at timestamp, retweet_count INT, 
		favorite_count INT, favorited boolean, retweeted boolean, filter_level text, lang text, text text, 
		source text, truncated boolean)")

	for (i in c(1:nrow(retweeted_df))) {
		insert_str = paste("INSERT INTO retweeted(id, create_at, retweet_count, favorite_count, favorited, ",
 			"retweeted, filter_level,lang, text, source, truncated) VALUES(",
			"'", retweeted_df$id[i], "', ",
			"'", retweeted_df$create_at[i], "', ",
			retweeted_df$retweet_count[i], ", ",
			retweeted_df$favorite_count[i], ", ",
			retweeted_df$favorited[i], ", ",
			retweeted_df$retweeted[i], ", ",
			"'", retweeted_df$filter_level[i], "', ",
			"'", retweeted_df$lang[i], "', ",
			"'", str_replace_all(retweeted_df$text[i], "'", ""), "', ",
			"'", retweeted_df$source[i], "', ",
			retweeted_df$truncated[i],
			")", sep = "")
		try(dbSendQuery(con, insert_str), silent=T)
	}
}

get_top_retweets <- function() {
	dbSendQuery(con, "DROP TABLE IF EXISTS top_daily_retweeted")
	dbSendQuery(con, "CREATE TABLE top_daily_retweeted(id VARCHAR(20) PRIMARY KEY, retweet_count INT)")

	retweet_ids_df <- as.data.frame(table(retweet_df$retweeted_status_id_str))
	retweet_ids_df <- retweet_ids_df[order(-retweet_ids_df$Freq),]
	
	id_v = as.vector(t(retweet_ids_df$Var1))
	freq_v = as.vector(t(retweet_ids_df$Freq))
	for (i in c(1:min(20,nrow(retweet_ids_df)))) {
		insert_str = paste("INSERT INTO top_daily_retweeted(id, retweet_count) VALUES(",
			"'", id_v[i], "', ", freq_v[i], ")", sep = "")
		try(dbSendQuery(con, insert_str), silent=T)
	}
}

daily_list = NULL
stream = NULL
tweet_df = NULL
retweet_df = NULL
retweeted_df = NULL
num_tweets = 0
num_retweets = 0
num_users = 0

init()
parse_daily_stream()
create_data_frames()
db_write_retweeted()
get_top_retweets()
#warnings()

dbDisconnect(con)

print("Finished analyzing twitter stream")