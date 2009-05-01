require "time_series"
require "csv"

data_file = "../data/DVS_Bodo_xyz/Bodo_xyz_cleaned_v2.csv" 
timeseries  = TimeSeries.new(data_file)      
# GA-2476Stn27GR12_evt_cleaned_v4.csv < do this later has different headers
# GA-2476Stn28GR13_evt_cleaned.csv 
# GA-2476Stn29GR14_evt_cleaned.csv 
# GA-2476_STN33GR017.csv 
# GA-2476_STN34GR018.csv 
# GA2476_031GR015.csv 
# GA2476_032GR016.csv

%w{ GA-2476Stn27GR12_evt_cleaned_v4.csv
    GA-2476Stn28GR13_evt_cleaned.csv 
    GA-2476Stn29GR14_evt_cleaned.csv 
    GA-2476_STN33GR017.csv 
    GA-2476_STN34GR018.csv 
    GA2476_031GR015.csv 
    GA2476_032GR016.csv
}.each do |file|
  lat_idx = 2
  lng_idx = 3
  if file == "GA-2476Stn27GR12_evt_cleaned_v4.csv"
    lat_idx = 4
    lng_idx = 5    
  end
  outputfile = "../output/#{File.basename(file, File.extname(file))}_processed#{File.extname(file)}"
  writer = CSV.open(outputfile, "w")
  reader = CSV.open("../data/#{file}", "r")
  header = reader.shift
  header << "Depth" 
  writer << header
  reader.each do |row|
    dd, mm, yyyy = row[0].split("/")                                  # get date
    hr, min, sec = row[1].split(":")                                  # get time
    hr = hr.to_i; min = min.to_i; sec = sec.to_i                      # set h m s to ints
    date = Date.parse("#{yyyy}/#{mm}/#{dd}")                          # Date obj
    time = Chronic.parse("#{"%02d"%hr}:#{"%02d"%min}:#{"%02d"%sec}")  # Time obj
    return "parse error: #{yyyy}/#{mm}/#{dd}\n #{row}" if date.nil? 
    return "parse error: #{"%02d"%hr}:#{"%02d"%min}:#{"%02d"%sec}\n #{row}" if time.nil?
    #STDOUT.print "about to look for #{yyyy}/#{mm}/#{dd} #{row[1]} #{row}\n"
    data = timeseries.closest_to(date, time, 5.0)
    if !data.nil?     
      STDOUT.print "#{file}: #{row[lat_idx]},#{row[lng_idx]} => #{data["Lat"]},#{data["Long"]}\n"
      row[lat_idx] = data["Lat"]
      row[lng_idx] = data["Long"]        
    elsif data = timeseries.closest_to(date, time, 10.0) 
      STDOUT.print "#{file}: #{row[lat_idx]},#{row[lng_idx]} => #{data["Lat"]},#{data["Long"]}\n"
      row[lat_idx] = data["Lat"]
      row[lng_idx] = data["Long"]              
    elsif data = timeseries.closest_to(date, time, 15.0) 
      STDOUT.print "#{file}: #{row[lat_idx]},#{row[lng_idx]} => #{data["Lat"]},#{data["Long"]}\n"
      row[lat_idx] = data["Lat"]
      row[lng_idx] = data["Long"]              
    else
      STDERR.print("NOT FOUND: #{file} #{"%02d"%hr}:#{"%02d"%min}:#{"%02d"%sec} #{row.join(", ")}\n")      
    end
    row << data["Depth"] unless data.nil?
    writer << row
  end
  writer.close
  reader.close
end
