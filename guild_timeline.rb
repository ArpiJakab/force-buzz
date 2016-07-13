require "twitter"
require "json"
require 'sequel'
require 'pg'

module TOS
	class GuildTimeline
		def initialize()
			@DB = Sequel.connect(ENV['DATABASE']) 
			@top_daily_retweeted = @DB[:top_daily_retweeted] # Create a dataset

			# @ArpiJakab
			@client = Twitter::REST::Client.new do |config|
			  config.consumer_key        = ENV['TWITTER_AJ_CONSUMER_KEY']
			  config.consumer_secret     = ENV['TWITTER_AJ_CONSUMER_SECRET']
			  config.access_token        = ENV['TWITTER_AJ_ACCESS_TOKEN']
			  config.access_token_secret = ENV['TWITTER_AJ_ACCESS_TOKEN_SECRET']
			end
			
			@collection_id = ENV['SALESFORCE_GUILD_COLLECTION_ID']
		end
		
		def update
			request = Twitter::REST::Request.new(@client, "get", "1.1/collections/entries.json", 
				{"id" => @collection_id, "count" => 200}) 
			entries = request.perform()
			collection = Set.new()
			entries[:objects][:tweets].each do |tweet|
			#entries['objects']['tweets'].each do |tweet|
				#puts tweet[0]
				collection.add(tweet[0].to_s)
			end

			@top_daily_retweeted.all.each do |retweeted|
				#puts collection.include? retweeted[:id]
				if (collection.include? retweeted[:id]) == false
					# must be retweeted more than once by another guild member within 24 hours
					if (retweeted[:retweet_count] > 1)
						# TODO: sort by create_at 
						puts "adding #{retweeted[:id]}"
						request = Twitter::REST::Request.new(@client, "post", "1.1/collections/entries/add.json", 
							{"id" => @collection_id, "tweet_id" => retweeted[:id]}) 
						result = request.perform()
						puts result
					end
				end
			end
		end
	end
end


