require "rubygems"
require "pp"
require "csv"
require "chronic"
require "fastercsv"
require "fileutils"

class TimeSeries
  attr_accessor :data, :format, :orig_data_file, :normalized_data_file   
  
  def initialize(csv_file, ops={})
  
    default_ops = {:format => "CSV", :cache => nil, :fast_index => nil}
    ops = default_ops.merge(ops)
    @cache  = ops[:cache]  
    @format = ops[:format]  
    @fast_index = ops[:fast_index]
    @original_data_file = "data.#{@format}"
    @normalized_data_file = "data.norm"
    @normalized_file_delim = " "
    
    raise "Fast index requested but root directory for cache not provided" if (not @fast_index.nil?) and @cache.nil?
   
    # init reader and headers
    if @format == "CSV"
      @reader    = FasterCSV.open(csv_file, "r")
      @headers  = @reader.shift 
      @headers.map!{|h| h.gsub(/ /, "_")}
      @delim = ","
    elsif @format == "ASCII"
      @reader    = FasterCSV.open(csv_file, "r", :col_sep=>" ", :skip_blanks=>true)
      @headers_ascii  = "year, julian_day, hour, minute, second, lon, lat, depth"
      @headers  = %w{ID UTC_Date UTC_Time Long Lat Depth}
      @delim = " "
    else
      raise "Unknow Timeseries Data Format"
    end
  
    @data = {} 
  end
  
  def TimeSeries.find(params={})
    raise "No Cache given" if params[:cache].nil?
    headers  = %w{ID UTC_Date UTC_Time Long Lat Depth}
    
    reverse = true
    looking_for =  string2datetime_with_frac(params[:looking_for], reverse) 

    cache = params[:cache]

    diff = params[:diff]
    key = params[:key]
    last_diff = diff.abs
    last_closest_row = nil  
    puts "-------\nLooking For #{looking_for} in #{File.join(cache, key)}"

    return nil if not File.exists?(File.join(cache, key))
    FasterCSV.foreach( File.join(cache, key, "data.norm"), :col_sep=>" ", :skip_blanks=>true) do |r|
      datetime = TimeSeries.normalize_time(r, true)
      curr_diff = (datetime - looking_for).abs 
      if curr_diff < last_diff 
        last_diff = curr_diff
        last_closest_row = r
      end        
    end
    return nil if last_closest_row.nil?
    last_closest_row_hash = headers.zip(last_closest_row).inject({}) do |hash, key_val|
      hash.merge!({key_val[0] => key_val[1]})
    end        
    last_closest_row_hash
  end
  
  def write_cache(ops={})
#    @cache = ops[:cache] || @cache 
    # TODO: Should be able to write a cache 
  end
  
  def load_data
    id = "0"
    is_rerun =  false
    cache_exists = false 
    cache_orig_exists = false
    cache_norm_exists = false
    
    is_rerun  = true if (@reader.lineno > 1 and @format == "CSV") or (@reader.lineno > 1 and @format == "ASCII") # this is a re-run 
#    p "rerun #{@reader.lineno} #{@format}" if is_rerun    
    if is_rerun
      last_run_lineno = @reader.lineno
      @reader.rewind 
    end
    
    @reader.each do |row|
      # normalize row and get the date and timestamp to use as hash keys
      row_orig = row
      
      norm_row = nomarlize_row(row, id.succ!)
      
      d, hr, min, sec = TimeSeries.normalize_time(norm_row)  
    
      unless @cache.nil? # process cache
        current_key   = File.join(d.year.to_s, d.strftime("%m"), d.strftime("%d"), hr, min) 
        current_cache = File.join(@cache, @format, current_key) 
        if is_rerun and File.exists?(File.join(current_cache, @original_data_file));    cache_orig_exists = true; else cache_orig_exists = false; end
        if is_rerun and File.exists?(File.join(current_cache, @normalized_data_file));  cache_norm_exists = true; else cache_norm_exists = false; end
        cache_exists = (cache_norm_exists and cache_orig_exists)
        begin
          FileUtils.mkdir_p(current_cache) unless File.exists?(current_cache)
          File.open(File.join(current_cache, @original_data_file), "a"){|f| f.puts row_orig.join(@delim)} unless cache_orig_exists
          File.open(File.join(current_cache, @normalized_data_file), "a"){|f| f.puts norm_row.join(@normalized_file_delim)} unless cache_norm_exists
        rescue SystemCallError => e
          puts "#{e} Failed to created #{current_cache}"
          FileUtils.rm_rf(current_cache)
        end    
      end
      
      row_hash = @headers.zip(norm_row).inject({}) do |hash, key_val|
        hash.merge!({key_val[0] => key_val[1]})
      end        

      if(@fast_index)
        if @data[current_key].nil?
          @data[current_key] = [row_hash] 
        else        
          @data[current_key] << row_hash 
        end
      else
        if @data[d].nil? 
          # new date - need to create a new date and hour hash + array for the rows
          @data.merge!( {d => {hr.to_i => [row_hash]}} )
        elsif @data[d][hr.to_i].nil? 
          # existing date but new hour - need to create the hour hash + array for the row
          @data[d].merge!({hr.to_i => [row_hash]})
        else 
          # just add row to existing date/hour
          @data[d][hr.to_i] << row_hash
        end
      end

    end    
  end
  
  def closest_to(params) 
    default_params = {:looking_for => nil, :time_key => nil, :date => nil, :time => nil, :diff => 0}
    params = default_params.merge(params)
    # determin if we are in fast mode
    fast_mode = (params[:time_key] and @cache)    
    diff = params[:diff]        
    looking_for =  Chronic.parse(params[:looking_for]) 
    last_diff = diff.abs
    last_closest_row = nil          
    if fast_mode
      key = params[:time_key] 
      return nil if @data[key].nil?
      data_subset = @data[key]
      data_subset.each do |r|
        a = [r["ID"], r["UTC_Date"], r["UTC_Time"], r["Long"], r["Lat"], r["Depth"]]
        datetime = TimeSeries.normalize_time(a, true)
        curr_diff = (datetime - looking_for).abs 
        if curr_diff < last_diff 
          last_diff = curr_diff
          last_closest_row = r
        end        
      end
      
    else      
      date = params[:date]
      time = params[:time]
      return nil if @data[date].nil?
      data_subset = @data[date][time.hour]
      return nil if data_subset.nil?
      looking_for = Chronic.parse("#{date.year}/#{date.mon}/#{date.day} #{"%02d"%time.hour}:#{"%02d"%time.min}:#{"%02d"%time.sec}")
      looking_for = looking_for + time.to_f.modulo(1)
      data_subset.each do |row|
        dd, mm, yyyy = row["UTC_Date"].split("/")  
        hr, min, sec = row['UTC_Time'].split(":")
        hr = hr.to_i; min = min.to_i; sec_int = sec.to_f.div(1); sec_frac = sec.to_f.modulo(1)
        datetime = Chronic.parse("#{yyyy}/#{mm}/#{dd} #{"%02d"%hr}:#{"%02d"%min}:#{"%02d"%sec_int}")
        datetime = datetime + sec_frac
        curr_diff = (datetime - looking_for).abs 
        if curr_diff < last_diff 
          last_diff = curr_diff
          last_closest_row = row 
        end
      end
    end    
    return last_closest_row
  end
  
  private
  # take a normalized row and return a the time stamp that will form the hash (date-object, hr, min, sec)
  def TimeSeries.normalize_time(normalized_row, datetime = false)

    dd, mm, yy = normalized_row[1].split("/")
    h, m, s = normalized_row[2].split(":") 
    h = h.rjust(2,"0"); m = m.rjust(2,"0"); s = s.rjust(2,"0")
    dt = TimeSeries.string2datetime_with_frac("#{normalized_row[1]} #{normalized_row[2]}")    
    return dt if datetime
    d = Date.parse("#{yy}/#{mm}/#{dd}")
    [d, h, m, s]
  end
  
  # take a row and return a normalize version based on @headers
  def nomarlize_row(row, row_num) 
    return row if @format == "CSV"
    yy, yday, h, m, s = row[0..4]
    # create an array of values conforming to csv schema inorder to zip to the headers
    d = Date.ordinal(y = yy.to_i, d=yday.to_i)
    t = Chronic.parse("#{h}:#{m}:#{s.to_f.div(1).to_s.rjust(2,'0')}")
    raise "TimeFormatError: Could not parse time #{h}:#{m}:#{s}" if t.nil?
    t = t + s.to_f.modulo(1)
    t = t.strftime("%H:%M:#{sec_with_fraction(t)}")
    row =  ["#{row_num}", "#{d.strftime("%d/%m/%Y")}", "#{t}", "#{row[5]}", "#{row[6]}", "#{row[7]}"]    
    row
  end
  
  def sec_with_fraction(time)    
    sec = (time.sec + time.to_f.modulo(1)).to_s
    i,f = sec.split(".")
    i.rjust(2, '0')+"."+f   # nn.ff
  end
  
  # takes date time as a string with format
  # "DD/MM/YYYY HR:MIN:SEC.DEC" (date_time_str) and 
  # returns a datetime object 
  def self.string2datetime_with_frac(date_time_str, reverse = false)

    date, time = date_time_str.split(" ")
    dd, mm, yy = date.split("/")
    yy, mm, dd = date.split("/") if reverse
    hr, min, sec = time.split(":") 
    hr = hr.rjust(2,"0"); min = min.rjust(2,"0"); sec = sec.rjust(2,"0")
    sec_int, sec_fraction = sec.split(".")

    dt_int = Chronic.parse("#{yy}/#{mm}/#{dd} #{hr}:#{min}:#{sec_int}")
    dt = dt_int + sec.to_f.modulo(1)
    dt
  end
  
end