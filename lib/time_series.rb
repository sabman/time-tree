require "rubygems"
require "pp"
require "csv"
require "chronic"

class TimeSeries
  attr_accessor :data   
  
  def initialize(csv_file)    
    reader    = CSV.open(csv_file, "r")
    @headers  = reader.shift
    @headers.map!{|h| h.gsub(/ /, "_")}
    @data     = {}    
    reader.each do |row|
      dd, mm, yy = row[1].split("/")
      d = Date.parse("#{yy}/#{mm}/#{dd}")
      row_hash = @headers.zip(row).inject({}) do |hash, key_val|
        hash.merge!({key_val[0] => key_val[1]})
      end
      hh, mm, ss = row[2].split(":")
      if @data[d].nil? 
        # new date - need to create a new date and hour hash + array for the rows
        @data.merge!( {d => {hh.to_i => [row_hash]}} )
      elsif @data[d][hh.to_i].nil? 
        # existing date but new hour - need to create the hour hash + array for the row
        @data[d].merge!({hh.to_i => [row_hash]} )
      else 
        # just add row to existing date/hour
        @data[d][hh.to_i] << row_hash
      end
    end
  end
  
  def closest_to(date, time, diff)    
    return nil if @data[date].nil?
    data_subset = @data[date][time.hour]
    looking_for = Chronic.parse("#{date.year}/#{date.mon}/#{date.day} #{"%02d"%time.hour}:#{"%02d"%time.min}:#{"%02d"%time.sec}")
    data_subset.each do |row|
      dd, mm, yyyy = row["UTC_Date"].split("/")  
      hr, min, sec = row['UTC_Time'].split(":")
      hr = hr.to_i; min = min.to_i; sec = sec.to_i
      datetime = Chronic.parse("#{yyyy}/#{mm}/#{dd} #{"%02d"%hr}:#{"%02d"%min}:#{"%02d"%sec}")
      return row if (datetime - looking_for).abs <= diff
    end
    return nil
  end
end