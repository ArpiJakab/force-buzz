require 'tweetstream'
require 'sequel'
require 'pg'

module TOS
	class TwitterStream
		def initialize()
			@DB = Sequel.connect(ENV['DATABASE']) 

			if @DB.table_exists?(:daily_stream) == false
				@DB.create_table :daily_stream do
					primary_key :id, :type=>String, :size=>20
					Json :status
				end
			end		
			
			@daily_stream = @DB[:daily_stream] # Create a dataset

			# @TestDataNow credentials
			TweetStream.configure do |config|
				config.consumer_key       = ENV['TWITTER_TDN_CONSUMER_KEY']
				config.consumer_secret    = ENV['TWITTER_TDN_CONSUMER_SECRET']
				config.oauth_token        = ENV['TWITTER_TDN_ACCESS_TOKEN']
				config.oauth_token_secret = ENV['TWITTER_TDN_ACCESS_TOKEN_SECRET']
				config.auth_method        = :oauth
			end
		end

		def record
			# Use 'userstream' to get message from your stream
			client = TweetStream::Client.new

			begin
				client.userstream do |status|
					puts "status id = #{status.id}"#, #{status.attrs}"
					@daily_stream.insert(:id => "#{status.id}", :status => JSON.generate(status.attrs))
				end		
			rescue EvenMachine::ConnectionError => e
				puts "Twitter stream API error " + e
			end
		end
	end
end