require File.dirname(__FILE__) + '/../spec_helper'
require "fastercsv"
require "pp"
require "chronic"
require File.dirname(__FILE__) + "/../../lib/time_series.rb"

describe "TimeSeries: " do

  describe "#data" do
    before do
      @data_file = "data/DVS_Bodo_xyz/Bodo_xyz_cleaned_cut_v2.csv"
      reader = FasterCSV.open(@data_file, 'r')
      @header = reader.shift
      @ts = TimeSeries.new(@data_file) 
      @ts.load_data
      reader.close
    end
  
  #  it "should read the time series data from a CSV file containing date and time colums"   
  #  it "should accept the date and time column numbers"  
  #  it "should assume the date is in dd/mm/yyyy format"
  #  it "should assume the time is hh:mm:ss format"
  #  it "should show the time range for the data, based-on date passed to it"   
      
    it "should create a hash for the data indexed by Date, then hour (which is a Fixnum), then the data" do
      @ts.data[Date.parse("2008/12/3")].class.should == Hash
    end 
    
    it "should read and store all the data in a hash called data" do
      @ts.data.values.size == 40    
    end
  end

  
  
  describe "TimeSeries.find(:looking_for => l, :time_key => k, :cache => c, :diff => d}" do
    it "should have a class method called find for preexisting cache" do
      FileUtils.rm_rf("cache")
      ts = TimeSeries.new("data/GA0308_MBnav.asc", :format => "ASCII", :cache => "cache", :fast_index => true)
      ts.load_data
      time_key = File.join("2008","08","13","11","19")
      looking_for = "2008/08/13 11:19:58.0050001144409"
      cache = File.join("cache", "ASCII")
      lambda{TimeSeries.find(:looking_for => looking_for, :key => time_key, :cache => cache, :diff => 5)}.should_not raise_error
      TimeSeries.find(:looking_for => looking_for, :key => time_key, :cache => cache, :diff => 5).should == {
        "ID" => "23", "UTC_Date" => "13/08/2008", 
        "UTC_Time" => "11:19:58.0050001144409",  
        "Long" => "114.141506", "Lat" => "-21.957321", 
        "Depth" => "-5.196"
        }
      FileUtils.rm_rf("cache")        
    end
  end
  
  describe "caching the time series" do
    before do
      @cache_dir  = "cache"
      @data_file  = "data/GA0308_MBnav.asc"
      @format = "ASCII"
      # setup cache dir and data hash for use in verifying they are created
      @cache_hash = init_cache_hash(:cache_dir => @cache_dir, :data_file => @data_file, :format => @format)
    end
    
    it "should take the cache directory as an option hash" do
      lambda { @ts = TimeSeries.new(@data_file, :format => @format, :cache => @cache_dir) }.should_not raise_error
    end
    
    it "should create the cache dir if it doesn't exist" do
      FileUtils.rm_rf(@cache_dir)
      @ts = TimeSeries.new(@data_file, :format => "ASCII", :cache => @cache_dir)   
      @ts.load_data
      subdirs = init_cache_subdirs(:cache_root_dir => @cache_dir, :cached_hash => @cache_hash, :format => @format)      
      subdirs.each{|dir| File.exists?(dir).should be_true}
    end
    
    describe "writing data in cache" do
      before do
        @headers = %w{year julian_day hour minute second lon lat depth}
        @norm_headers  = %w{ID UTC_Date UTC_Time Long Lat Depth}
        @cache_dir = "cache"
        @data_file  = "data/GA0308_MBnav.asc"
        @format = "ASCII"; @delim = " "
        @cache_hash = init_cache_hash(:cache_dir => @cache_dir, :data_file => @data_file, :format => @format)
        @cache_subdirs = init_cache_subdirs(:cache_root_dir => @cache_dir, :cached_hash => @cache_hash, :format => @format)
        FileUtils.rm_rf(@cache_dir)
        @opt_params = {:format => @format, :cache => @cache_dir, :fast_index => true}
        @norm_data_file = "data.norm"
        @orig_data_file = "data.#{@format}"
      end
      it "should write a data file to each of the cache dirs with the data in the 'normalized format'" do        
        @ts = TimeSeries.new(@data_file, :format => "ASCII", :cache => @cache_dir); @ts.load_data        
        @cache_subdirs.sort.each do |dir|
          data_file = File.join(dir, @norm_data_file)                   
          File.exists?(data_file).should be_true
          FasterCSV.foreach(data_file, :col_sep => @delim, :skip_blanks=>true) do |row|
            key = dir.gsub(File.join("cache", "ASCII", "/"), '')
            @cache_hash[@format][key].should include(row)
          end
        end
      end
      
      it "should take fast_index parameter which is only valid if there is a cache" do
        lambda{TimeSeries.new(@data_file, @opt_params)}.should_not raise_error
        lambda{TimeSeries.new(@data_file, :format => @format, :fast_index => true)}.should raise_error("Fast index requested but root directory for cache not provided")
      end
      
      it "should allow fast indexing by creating an index on cache dir string"  do
        @ts = TimeSeries.new(@data_file, @opt_params); @ts.load_data
        @cache_subdirs.each do |dir|          
          data_file = File.join(dir, @norm_data_file)
          FasterCSV.foreach(data_file, :col_sep => @delim, :skip_blanks=>true) do |row|
            key = dir.gsub(File.join("cache", "ASCII", "/"), '')
            @ts.data[key].should include(row2hash(@norm_headers, row))  
          end 
        end      
      end
      it "should write a data.norm containing normalized data and data.<format> file with original format" do 
        @ts = TimeSeries.new(@data_file, @opt_params); @ts.load_data
        @cache_subdirs.each do |dir|
          File.exists?(File.join(dir,@norm_data_file)).should be_true
          File.exists?(File.join(dir,@orig_data_file)).should be_true        
        end
      end

      it "should handle incomplete cache by reading from the cache dir where possible and the rest from file and populating the cache" do
        @ts = TimeSeries.new(@data_file, @opt_params)
        @ts.load_data
        missing_cache_dirs = @cache_subdirs[@cache_subdirs.size-3..@cache_subdirs.size-1]
        missing_cache_dirs.each {|dir| FileUtils.rm_rf(dir)}
        complete_cache_dirs = @cache_subdirs - missing_cache_dirs
        
        complete_cache_dirs.each{|d| File.exists?(d).should be_true}
        missing_cache_dirs.each{|d| File.exists?(d).should_not be_true}
        @ts.load_data
        @cache_subdirs.each do |dir|          
          File.exists?(dir).should be_true
          data_file = File.join(dir, @norm_data_file)
          FasterCSV.foreach(data_file, :col_sep => @delim, :skip_blanks=>true) do |row|
            key = dir.gsub(File.join("cache", "ASCII", "/"), '')
            @ts.data[key].should include(row2hash(@norm_headers, row))  
          end 
        end              
      end
    end 
  

    it "should write the cache completely and roll back if there is an interrupt"
    it "should load the cache into a hash if its already populated" 
    it "should populated the cache if its empty" 
    it 'should have a normalized schema with 
        {"UTC_Time" => "12:20:06.25500011444092",
         "UTC_Date" => "13/08/2008",
         "ID"       => "1",
         "Long"     => "114.141510",
         "Depth"    => "-5.102",
         "Lat"      => "-21.957306"}'

  end
  

  describe "#closest_to(:looking_for => t, :time_key => key, :diff => 2) with cache" do
    before do
      data_file = "data/DVS_Bodo_xyz/Bodo_xyz_cleaned_cut_v2.csv"
      format = "CSV"
      cache_dir = "cache"
      FileUtils.rm_rf(cache_dir)
      opt_params = {:format => format, :cache => cache_dir, :fast_index => true}
      @ts = TimeSeries.new(data_file, opt_params); @ts.load_data 
      @time_key = "2008/11/28/00/01"
      @looking_for = "2008/11/28 00:01:02"
    end
    it "should accept a string of yyyy/mm/dd/hr/min" do
#      pp @ts.data
      @ts.closest_to(:looking_for => @looking_for, :time_key => @time_key, :diff => 5.0).should == {"UTC_Time"=>"0:01:00", "UTC_Date"=>"28/11/2008", "ID"=>"7", "Long"=>"110.9364857", "Depth"=>"136", "Lat"=>"-25.7885335"}      
    end

  end

  describe "#closest_to(:date => d, :time => t, :diff => d)" do
    before do
      @data_file = "data/DVS_Bodo_xyz/Bodo_xyz_cleaned_cut_v2.csv"
      reader = FasterCSV.open(@data_file, 'r')
      @header = reader.shift
      @ts = TimeSeries.new(@data_file) 
      @ts.load_data
      reader.close      
      @date1 = Date.parse("2008/11/28")
      @date2 = Date.parse("2008/12/02")
      @time1 = Chronic.parse("00:01:02")
      @time2 = Chronic.parse("00:00:27")      
      @time3 = Chronic.parse("00:00:35")
      @time4 = Chronic.parse("11:33:39")
      @time5 = Chronic.parse("11:33:59")      
    end
    
    it "should fetch the row which is closest in time to the passed date, time and difference between times parameters" do
      @ts.closest_to(:date => @date1, :time => @time1, :diff => 5.0).should == {"UTC_Time"=>"0:01:00", "UTC_Date"=>"28/11/2008", "ID"=>"7", "Long"=>"110.9364857", "Depth"=>"136", "Lat"=>"-25.7885335"}
      @ts.closest_to(:date => @date1, :time => @time2, :diff => 5.0).should == {"UTC_Time"=>"0:00:30", "UTC_Date"=>"28/11/2008", "ID"=>"4", "Long"=>"110.9364653", "Depth"=>"119", "Lat"=>"-25.788484"}      
      @ts.closest_to(:date => @date2, :time => @time4, :diff => 5.0).should == {"UTC_Time"=>"11:33:40", "UTC_Date"=>"2/12/2008", "ID"=>"3723", "Long"=>"112.7628032", "Depth"=>"40", "Lat"=>"-21.911783"}
      @ts.closest_to(:date => @date2, :time => @time5, :diff => 5.0).should == {"UTC_Time"=>"11:34:00", "UTC_Date"=>"2/12/2008", "ID"=>"3725", "Long"=>"112.7627144", "Depth"=>"52", "Lat"=>"-21.9115508"}
    end
    it "should return nil if the passed date, time and difference params fall outside of the data range or density" do
      @ts.closest_to(:date => @date1, :time => @time1, :diff => 1.0).should == nil
    end
  end
  
  describe "file-format adapter" do
    before do
      @headers = %w{year julian_day hour minute second lon lat depth}
      @ts   = nil
      @data_file  = "data/GA0308_MBnav.asc"
      File.open(@data_file, "r") do |f|
        @lines = f.readlines
      end
      @data_file2  = "data/GA0308_MBnav_rounded.asc"
      File.open(@data_file2, "r") do |f|
        @lines2 = f.readlines
      end      
    end
    it "should accept a format parameter in options hash to workout the input file format" do
      lambda{TimeSeries.new(@data_file, :format => "ASCII")}.should_not raise_error
    end
    it "should find the closest lat lon" do
      @ts = TimeSeries.new(@data_file, :format => "ASCII")
      @ts.load_data
      i = 1
      @lines.each do |row|
        row = row.split(" ")
        date, time = ascii_row2closest_to_params(row, @headers)
        @ts.closest_to(:date => date, :time => time, :diff => 4.0).should == ascii_row2schema_hash(row, @headers, i)
        i = i+1
      end      

      @ts = TimeSeries.new(@data_file2, :format => "ASCII")
      @ts.load_data      
      i = 1
      @lines2.each do |row|        
        row = row.split(" ")
        date, time = ascii_row2closest_to_params(row, @headers)
        @ts.closest_to(:date => date, :time => time, :diff => 4.0).should == ascii_row2schema_hash(row, @headers, i)
        i = i+1
      end      
    end    
  end
  
  private

  def init_cache_subdirs(params)
    #:cache_root_dir => @cache_dir, :cached_hash => @cache_subdirs, :format => @format
    cache_dir = params[:cache_root_dir]  
    cache_subdirs = params[:cached_hash] 
    format = params[:format]
    dirs = []
    cache_subdirs[format].each do |k,v| 
      dirs << File.join( "#{cache_dir}","#{format}","#{k}")    
    end    
    dirs
  end
  
  def init_cache_hash(params)
    format    = params[:format]
    data_file = params[:data_file]
    
    cache_hash = {format => {}}
    hdr = %w{year julian_day hour minute second lon lat depth}
    i = "0"
    FasterCSV.foreach(data_file, :col_sep => " ") do |row|
      h = ascii_row2schema_hash(row, hdr, i.succ!) 
      normalized_row = [h["ID"], h["UTC_Date"], h["UTC_Time"], h["Long"], h["Lat"], h["Depth"] ]
      #  "UTC_Time" => t.strftime("%H:%M:#{dec_sec}"), 
      #  "UTC_Date" => d.strftime("%d/%m/%Y"), 
      #  "ID"    => "#{id}", 
      #  "Long"  => "#{r[h.index('lon')]}", 
      #  "Depth" => "#{r[h.index('depth')]}",
      #  "Lat"   => "#{r[h.index('lat')]}" 
      #
      d, m, y       = h["UTC_Date"].split('/')
      hr, min, sec  = h["UTC_Time"].split(':') 
      curr_cache_dir = File.join( "#{y}","#{m}","#{d}","#{hr}","#{min}" )
      
      if cache_hash[format].keys.include?("#{curr_cache_dir}")
        cache_hash[format]["#{curr_cache_dir}"] << normalized_row
      else
        cache_hash[format].merge!({"#{curr_cache_dir}" => [normalized_row]})
      end      
    end
    cache_hash
  end
  
  
  def ascii_row2closest_to_params(r, h)
    s = r[h.index('second')] 
    t = "#{r[h.index('hour')]}:#{r[h.index('minute')]}:#{s.to_f.div(1).to_s.rjust(2,'0')}"
    d = Date.ordinal(y=r[h.index("year")].to_i, d=r[h.index("julian_day")].to_i)
    t = Chronic.parse(t) 
    raise "TimeFormatError: Could not parse time #{r[h.index('hour')]}:#{r[h.index('minute')]}:#{s}" if t.nil?
    t = t + s.to_f.modulo(1)
    [d, t]
  end
  
  def ascii_row2schema_hash(r, h, id)
    t = "#{r[h.index('hour')]}:#{r[h.index('minute')]}:#{r[h.index('second')].to_f.div(1).to_s.rjust(2,'0')}"
    raise "TimeFormatError: Could not parse time #{r[h.index('hour')]}:#{r[h.index('minute')]}:#{r[h.index('second')]}" if t.nil?
    t = Chronic.parse(t)    
    t = t + r[h.index('second')].to_f.modulo(1) # add fractional seconds
    dec_sec = sec_with_fraction(t)  # create seconds a string with the fractions
    d = Date.ordinal(y=r[h.index("year")].to_i, d=r[h.index("julian_day")].to_i)    
    {
      "ID"    => "#{id}",     
      "UTC_Date" => d.strftime("%d/%m/%Y"), 
      "UTC_Time" => t.strftime("%H:%M:#{dec_sec}"), 
      "Long"  => "#{r[h.index('lon')]}", 
      "Lat"   => "#{r[h.index('lat')]}",
      "Depth" => "#{r[h.index('depth')]}"
    }
  end
  
  def ascii_row2normalized_row(r, h, id)     
    hash = ascii_row2schema_hash(r, h, id)
    [hash["ID"], hash["UTC_Date"], hash["UTC_Time"], hash["Long"], hash["Lat"], hash["Depth"] ]
  end
  
  def sec_with_fraction(time)    
    sec = (time.sec + time.to_f.modulo(1)).to_s
    i,f = sec.split(".")
    i.rjust(2, '0')+"."+f
  end
  
  def row2hash(keys, values)
    row_hash = keys.zip(values).inject({}) do |hash, key_val|
      hash.merge!({key_val[0] => key_val[1]})
    end   
    row_hash
  end
end