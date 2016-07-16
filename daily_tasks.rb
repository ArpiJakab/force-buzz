# TODO
# Read all friends once a day into a DB table
# Once a day, scan friend's friends
#	Look for Salesforce guild folks based on bio
#	Look for people, multiple friends follow (likely are industry though leaders) 
# Continually read friends favorites
#	After processing one loop of friends, update the guild collection
# Continually read stream
#	Done. Check frequently in case it crashes an needs a restart
#	Figure out looping strategy (keeping stream daily)
#	Done. Persist to DB
# Once an hour run R analytics on streamed tweets
#	Write top trending tweets to DB
#	Run script to add hot tweets to the guild collection
#	
# Monitor tasks 
#	Send immediate error report by email if there is a failure. 
#	Send daily status reports:
#		# new friends, favorite
require_relative 'tos'

module TOS
	@@twitter_stream_is_recording = false
	@@last_run_db_cleanup = 0
	@@db_cleanup_schedule = 60*60*24 # daily
	@@last_run_update_timeline = 0
	@@update_timeline_schedule = 60*10 # every 10 minutes
	
	def self.start_tasks
		while (true)
			if @@twitter_stream_is_recording == false
				Thread.new { start_record_twitter_stream() }
			end
			
			db_cleanup()
			update_salesforce_guild_timeline()			
			sleep(60) 
		end
	end
	
	def self.start_record_twitter_stream
		puts 'Start: Record twitter stream task, ' + Time.now.inspect
		@@twitter_stream_is_recording = true
		begin
			ts = TwitterStream.new()
			ts.record()
		rescue Exception => e
			@@twitter_stream_is_recording = false
			puts "Twitter streaming failed with error: #{e} #{Time.now.inspect}"
			raise e
		end
		@@twitter_stream_is_recording = false
		puts 'Stop: Record twitter stream task, ' + Time.now.inspect
	end

	def self.db_cleanup
		if @@last_run_db_cleanup == 0 || Time.now - @@last_run_db_cleanup >= @@db_cleanup_schedule
			@@last_run_db_cleanup = Time.now
		else
			return
		end
		
		puts 'Start: DB cleanup task, ' + Time.now.inspect
		begin
			ts = TwitterStream.new()
			ts.delete_day_old_tweets()
		rescue Exception => e
			puts "DB cleanup failed with error: #{e} #{Time.now.inspect}"
			raise e
		end
		puts 'Stop: DB cleanup task, ' + Time.now.inspect
	end
	
	def self.update_salesforce_guild_timeline
		if @@last_run_update_timeline == 0 || Time.now - @@last_run_update_timeline >= @@update_timeline_schedule
			@@last_run_update_timeline = Time.now
		else
			return
		end

		puts 'Start: Update salesforce guild timeline task, ' + Time.now.inspect
		begin
			timeline = GuildTimeline.new()
			timeline.update()
		rescue Exception => e
			puts "Twitter collections API error: #{e} #{Time.now.inspect}"
			raise e
		end
		puts 'Stop: Update salesforce guild timeline task, ' + Time.now.inspect
	end	
end

TOS.start_tasks();
