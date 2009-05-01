require File.dirname(__FILE__) + '/../spec_helper'
require "csv"
require "pp"
require "chronic"
require File.dirname(__FILE__) + "/../../lib/time_series.rb"

describe "TimeSeries" do
  before do
    @data_file = "data/DVS_Bodo_xyz/Bodo_xyz_cleaned_cut_v2.csv"
    reader = CSV.open(@data_file, 'r')
    @header = reader.shift
    @ts = TimeSeries.new(@data_file)    
  end

  it "should read the time series data from a CSV file containing date and time colums"   
  it "should accept the date and time column numbers"  
  it "should assume the date is in dd/mm/yyyy format"
  it "should assume the time is hh:mm:ss format"
  
  it "should create a hash for the data indexed by Date, then hour (which is a Fixnum), then the data" do
    @ts.data[Date.parse("2008/12/3")].class.should == Hash
  end 
  
  it "should read and store all the data in a hash called data" do
    @ts.data.values.size == 40    
  end
  
  it "should show the time range for the data based on the date passed to it" do
    #pp @ts.data
  end

  describe "#closest_to(date, time, diff)" do
    before do
      @date1 = Date.parse("2008/11/28")
      @date2 = Date.parse("2008/12/02")
      @time1 = Chronic.parse("00:01:02")
      @time2 = Chronic.parse("00:00:27")      
      @time3 = Chronic.parse("00:00:35")
      @time4 = Chronic.parse("11:33:39")
      @time5 = Chronic.parse("11:33:59")      
    end
    
    it "should fetch the row which is closest in time to the passed date, time and difference between times parameters" do
      @ts.closest_to(@date1, @time1, 5.0).should == {"UTC_Time"=>"0:01:00", "UTC_Date"=>"28/11/2008", "ID"=>"7", "Long"=>"110.9364857", "Depth"=>"136", "Lat"=>"-25.7885335"}
      @ts.closest_to(@date1, @time2, 5.0).should == {"UTC_Time"=>"0:00:30", "UTC_Date"=>"28/11/2008", "ID"=>"4", "Long"=>"110.9364653", "Depth"=>"119", "Lat"=>"-25.788484"}      
      @ts.closest_to(@date2, @time4, 5.0).should == {"UTC_Time"=>"11:33:40", "UTC_Date"=>"2/12/2008", "ID"=>"3723", "Long"=>"112.7628032", "Depth"=>"40", "Lat"=>"-21.911783"}
      @ts.closest_to(@date2, @time5, 5.0).should == {"UTC_Time"=>"11:34:00", "UTC_Date"=>"2/12/2008", "ID"=>"3725", "Long"=>"112.7627144", "Depth"=>"52", "Lat"=>"-21.9115508"}
    end
    it "should return nil if the passed date, time and difference params fall outside of the data range or density" do
      @ts.closest_to(@date1, @time1, 1.0).should == nil
    end
  end
  
end