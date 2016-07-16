require 'sequel'
require 'pg'
require 'json'

DB = Sequel.connect(ENV['DATABASE_URL']) 

begin
	daily_stream = DB[:daily_stream] # Create a dataset

	daily_stream.each do |row|
		status = JSON.parse(row[:status])
		recorded_at = Time.strptime(status["created_at"], '%A %B %d %H:%M:%S %z %Y')
		id = status["id"]
		puts "row: #{id} #{recorded_at}"
		daily_stream.where(:id => "#{id}").update(:recorded_at => recorded_at)
	end
ensure
	DB.disconnect()
end			
