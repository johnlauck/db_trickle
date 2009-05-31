#!/usr/bin/env ruby

# DB Trickle Archiver (db_trickle.rb) v1.1.0
# 
# The MIT License
# 
# Copyright (c) 2007,2008 John M Lauck (john AT recaffeinated d0t com / isosceles @ rubyforge)
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

require 'rubygems'
require 'mysql'
require 'cooloptions'
require 'log4r'
require 'strscan'

include Log4r

# Date time method for parsing mysql datetime
module DateTimeImport
  
  PRINTABLE_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
  MYSQL_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
  LOGGER = Logger.new('log')
  LOGGER.level = DEBUG
  LOGGER.outputters = Outputter.stdout
  
  # Detects time in unix time stamp or mysql datetime format
  # and creates a ruby Time object
  def DateTimeImport.convert_time(time)
    case time.to_s

      # unix time
      when /^\d+$/
        LOGGER.info "Using unix time format for #{time.to_s}"
        get_time_from_unixtime(time.to_s)

      # mysql datetime
      when /^(\d{2,4})-(\d{1,2})-(\d{1,2}) (\d{1,2}):(\d{1,2}):(\d{1,2})$/
        LOGGER.info "Using mysql datetime format for #{time.to_s}"    
        get_time_from_mysql_datetime(time.to_s)
    else
      nil
    end
  end

  # convert a time from unix timestamp to ruby Time obj
  def DateTimeImport.get_time_from_unixtime(unixtime)
    rubytime = nil
  
    begin
      rubytime = Time.at(unixtime.to_i)
    rescue TypeError => te
      rubytime = nil
      LOGGER.error "Error in type conversion from unix time format."
    end
  
    rubytime
  end

  # Convert a time from mysql datetime to ruby time object
  def DateTimeImport.get_time_from_mysql_datetime(mysql_datetime)
    datetime_parser = StringScanner.new(mysql_datetime)
    datetime_parser.scan(/^(\d{2,4})-(\d{1,2})-(\d{1,2}) (\d{1,2}):(\d{1,2}):(\d{1,2})$/)
    year=datetime_parser[1]
    month=datetime_parser[2]
    day=datetime_parser[3]  
    hour=datetime_parser[4]
    min=datetime_parser[5]
    sec=datetime_parser[6]
    rubytime = nil
    
    begin 
      rubytime = Time.mktime(year, month, day, hour, min, sec, 0)
    rescue ArgumentError => ae
      LOGGER.error "Error converting time format."
      rubytime = nil
    end
    
    rubytime
  end

end

# ===========
# = Methods =
# ===========

# gets the schema of the source table
def get_schema_struct(table_name)
  dbres = do_sql_command("DESC #{table_name};")

  dbstruct = []

  if(dbres) then
    dbres.each_hash do | row |
      dbstruct_hash = {}
      row.each {|key, val|
        dbstruct_hash[key.downcase.to_sym] = val        
      }
      dbstruct << dbstruct_hash
    end 
  end

  dbstruct
end

# takes the slightly modified hash from a mysql result and creates a table schema
# this ignores keys other than the pk
def get_pkey_fields(table_struct)
  pkeys = []

  table_struct.each do | row |
    pkeys << row[:field] if row[:key] == 'PRI'
  end

  pkeys
end

# takes the slightly modified hash from a mysql result and creates a table schema
# this ignores keys other than the pk
def get_schema_sql(table_struct, table_name = NEW_TABLE_NAME)
  dbstruct = []
  pkeys = []

  table_struct.each do | row |
    dbstruct << "`#{row[:field]}` #{row[:type]} #{(!row[:default].nil? && row[:default] != '' ) ? "default '#{row[:default]}'" : ''} #{row[:null] == 'NO' ? 'NOT NULL' : ''}"
    pkeys << "`#{row[:field]}`" if row[:key] == 'PRI'
  end 

  dbstruct <<  "PRIMARY KEY (%s)" % [pkeys.join(', ')]
  dbstring = "CREATE TABLE `%s` (\n\t%s\n)" % [table_name, dbstruct.join(",\n\t")]

  dbstring
end

# take given a set of keys and a row return a hash for the primary key index of this row,
# this means whatever this returns can be used to lookup the row
def make_key_hash_for_row(keys, row)
  key_hash = {}
  keys.each {|k|
    key_hash[k] = row[k]
  }
  
  key_hash
end

# grabs the rows from the src db
def grab_rows(field, src_table_name = TABLE_NAME, num_rows = ROWS_PER_TRANSACTION)
  LOGGER.info "Creating select statement based on field `#{field[:name]}` (#{field[:type]})"
  
  if !(field[:type] =~ /int/).nil?
    LOGGER.info "Using integer type for select."
    sql = "SELECT * FROM `%s` WHERE `%s` >= '%s' AND `%s` < '%s' ORDER BY `%s` LIMIT %s;" % [ Mysql::escape_string(src_table_name), 
                                                                                              Mysql::escape_string(field[:name]), 
                                                                                              Mysql::escape_string(field[:min].to_i.to_s), 
                                                                                              Mysql::escape_string(field[:name]), 
                                                                                              Mysql::escape_string(field[:max].to_i.to_s), 
                                                                                              Mysql::escape_string(field[:name]), 
                                                                                              num_rows]
  elsif !(field[:type] =~ /datetime/).nil?
    LOGGER.info "Using datetime type for select."    
    sql = "SELECT * FROM `%s` WHERE `%s` >= '%s' AND `%s` < '%s' ORDER BY `%s` LIMIT %s;" % [ Mysql::escape_string(src_table_name), 
                                                                                              Mysql::escape_string(field[:name]), 
                                                                                              Mysql::escape_string(field[:min].strftime(MYSQL_DATETIME_FORMAT)), 
                                                                                              Mysql::escape_string(field[:name]), 
                                                                                              Mysql::escape_string(field[:max].strftime(MYSQL_DATETIME_FORMAT)), 
                                                                                              Mysql::escape_string(field[:name]), 
                                                                                              num_rows]
  else
    LOGGER.info "Using default type for select, this isn't expected."
    sql = "SELECT * FROM `%s` WHERE `%s` >= '%s' AND `%s` < '%s' ORDER BY `%s` LIMIT %s;" % [ Mysql::escape_string(src_table_name), 
                                                                                              Mysql::escape_string(field[:name]), 
                                                                                              Mysql::escape_string(field[:min]), 
                                                                                              Mysql::escape_string(field[:name]), 
                                                                                              Mysql::escape_string(field[:max]), 
                                                                                              Mysql::escape_string(field[:name]), 
                                                                                              num_rows]    
  end
  
  LOGGER.debug "SQL: #{sql}"
  dbres = do_sql_command(sql)
  dbres
end

# inserts rows and returns the rows to delete
def insert_rows(rows, field, table_struct, dest_table_name = NEW_TABLE_NAME)
  fields = get_fields(table_struct)
  insert_tmplt = row_sql_insert(dest_table_name, table_struct)
  primary_keys = get_pkey_fields(table_struct)       
  errs = []
  row_action_data = []
  del_keys = []
  
  if (rows) then
    rows.each_hash do | row |
     row_action_data << {
       :sql_insert => make_sql_insert_row(fields, insert_tmplt, row), 
       :key => make_key_hash_for_row(primary_keys, row)
     }
    end
  end

  row_action_data.each { |row|
    begin
      dbres = do_sql_command(row[:sql_insert])
      if dbres.nil?
       del_keys << row[:key]
      end
    rescue Mysql::Error
      if !($! =~ /^Duplicate entry .* for key/).nil?
        # i'll consider a duplicate entry okay for a delete
        LOGGER.warn "Database error! Duplicate key found on insert, marking for deletion anyway, moving on: #{$!}"
        del_keys << row[:key]
      else
        #errs << "Database error, moving on: #{$!}"
        LOGGER.error "Database error, not sure what, moving on: #{$!}"
      end
    end
  }

  del_keys
end

# copy rows from src table to dest table
def copy_rows(  field, 
                table_struct, 
                src_table_name = TABLE_NAME, 
                dest_table_name = NEW_TABLE_NAME, 
                num_rows = ROWS_PER_TRANSACTION)
  rows = grab_rows(field, src_table_name, num_rows)
  keys_for_delete = insert_rows(rows, field, table_struct, dest_table_name)
  keys_for_delete
end

# copies and then deletes rows in chunks of num_rows size
def move_rows(  field, 
                table_struct, 
                src_table_name = TABLE_NAME, 
                dest_table_name = NEW_TABLE_NAME, 
                num_rows = ROWS_PER_TRANSACTION, 
                max_rows = MAX_RECORDS, 
                sleepy_time = SLEEP_TIME)
  iteration = 1
  count = 0

  # prime the pump vars for loop
  keys_for_delete = []

  if max_rows != 0 && max_rows < num_rows
    LOGGER.info "Adjusting per row transaction due to maximum row limit.  This move will only require one transaction."
    upper_bound = max_rows
    num_rows = max_rows    
  else
    upper_bound = num_rows
  end

  lower_bound = 1    
  max_iterations = max_rows == 0 ? 0 : (max_rows.to_f/num_rows.to_f).ceil
  remaining_rows = max_rows % num_rows
  
  while ((iteration == 1 || !keys_for_delete.empty?) && (max_iterations == 0 || iteration <= max_iterations)) do
    # sleep if we need another iteration
    if iteration > 1
      LOGGER.info "Sleeping for #{sleepy_time}"
      sleep sleepy_time
    end

    LOGGER.debug "upper_bound: #{upper_bound}\nlower_bound: #{lower_bound}\nnum_rows: #{num_rows}\niteration: #{iteration}\nmax_rows: #{max_rows}\nmax_rows: #{max_iterations}"  
    
    LOGGER.info "Starting move transactions iteration #{iteration} (records #{lower_bound} to #{upper_bound})"

    LOGGER.info "Copying up to #{num_rows} rows..."
    keys_for_delete = copy_rows(field, table_struct, src_table_name, dest_table_name, num_rows)
    LOGGER.info "...done"

    if keys_for_delete.size > 0
      count += keys_for_delete.size
      LOGGER.info "Deleting #{keys_for_delete.size} rows..."
      delete_rows(keys_for_delete, src_table_name)
      LOGGER.info "...done\n"
    else
      LOGGER.info "No rows to delete.  This could be a problem, but should just mean that it's last iteration."
    end

    # do calculations for next iterations
    iteration += 1

    upper_bound = (iteration * num_rows)
    lower_bound = upper_bound - num_rows + 1

    # this is the last iteration
    if remaining_rows != 0 && iteration == max_iterations
      LOGGER.info "Last iteration, only a partial per transaction is needed."
      num_rows = remaining_rows
      upper_bound = max_rows
    end

  end
  
  count
end

def row_sql_delete(src_table_name)
  sql = <<-EOF
    DELETE FROM `#{src_table_name}` 
    WHERE 
      %s
    LIMIT 1;
  EOF
  
  sql
end

def make_where_clause(keys)
  clauses = []
  keys.each { |key|
    tmp_clause = []
    key.each { |i, val|
       tmp_clause << "`#{i}` = '#{Mysql::escape_string(val)}'"
    }
    clauses << tmp_clause.join(' AND ')
  }
  
  clauses
end

def make_sql_delete_rows(src_table_name, keys)
  whereless_sql = row_sql_delete(src_table_name)
  where_clauses = make_where_clause(keys)

  sql_deletes = []
  where_clauses.each {|where|
    sql_deletes << whereless_sql % where
  }
  
  sql_deletes
end

def delete_rows(keys, src_table_name = TABLE_NAME)
  errs = []
  sql_delete_statements = make_sql_delete_rows(src_table_name, keys)
    
  sql_delete_statements.each{ |sql|
    LOGGER.debug "DELETE SQL: #{sql}"
    begin
      dbres = do_sql_command(sql)
    rescue Mysql::Error
      LOGGER.error "Database error, moving on: #{$!}"
    end
  }
  
end

def get_fields(table_struct)
  fields = []
  table_struct.each do | row |
    fields.push << row[:field]
  end
  
  fields
end

def make_sql_insert_row(field_names, row_template, row)
  values  = []
  field_names.each { |field|
    values << Mysql::escape_string(row[field])
  }  
  sql = row_template % values

  sql
end

# makes a template for sql row inserts (based on the current schema)
def row_sql_insert(table_name, table_struct)
  fields = get_fields(table_struct)

  sql = <<-EOF
  INSERT INTO `#{DBNAME}`.`#{table_name}` (
    #{fields.collect { |f| "`#{f}`" }.join(", ")}
  )
  VALUES (
    #{fields.collect { |f| "'%s'" }.join(", ")}
  );
  EOF

  sql
end

# creates the new table (really just wrapps the do sql method, but make its readable)
# that new method kind of makes this useless
def create_new_table(table_sql)
  begin
    do_sql_command(table_sql)
    LOGGER.info "Successfully reated new table: \n#{table_sql}\n"
  rescue Mysql::Error
    if ($!.to_s =~ /^Table .* already exists$/) == 0
      LOGGER.warn "Database error, duplicate table is okay, moving on: #{$!}"
    else
      LOGGER.error "Database error, moving on: #{$!}"
    end
  end
end

# get the field type (useful to determine if a time field is datetime or timestamp/integer)
def get_field_datatype(table_struct, field_name = FIELD_NAME)
  datatype = nil
  
  table_struct.each { |row|
    break if !datatype.nil?
    if row[:field] == field_name
      datatype = row[:type]
    end
  }
    
  datatype
end

# optimize table
def optimize_table(table)
  result = do_sql_command("OPTIMIZE TABLE `#{Mysql::escape_string(table)}`")
  if !result['Msg_type'].nil? && !result['Msg_text'].nil?
    {:type => result['Msg_type'], :text => result['Msg_text']}
  else
    nil
  end
end

# just pass a sql command and this does it all and returns the result
def do_sql_command(sql, use_global_connection = true, close_connection = false)
  begin
    
    if use_global_connection 
      dbconn = open_conn
    else
      dbconn = open_conn(false)
    end
  
    dbres = dbconn.query(sql)

  rescue Mysql::Error => err
    raise err
  ensure
    close_conn(dbconn) if close_connection
  end
  
  dbres
end

# returns true if the global database connection is available 
def is_global_db_connected?
  if defined?($global_db_conn).nil? || $global_db_conn.class != Mysql || $global_db_conn.stat == 'MySQL server has gone away'
    return false
  end
  
  return true
end

# opens a database conneciton with the set params, uses a global connection by default but can create a new connection and return that instead
def open_conn(global = true, set_wait = true, host = DBHOST, user = DBUSER, pass = DBPASS, name = DBNAME)
  LOGGER.debug "Opening db connection"
  conn = nil

  #use global connection
  if global 
     LOGGER.debug "Using global database connection"
   
    # global connection is already defined
    if is_global_db_connected?
      LOGGER.debug "Global connection is set, just giving it back"
      conn = $global_db_conn
    
    # global connection is not defined or not set
    else
    
      # open new global connection  
      LOGGER.debug "Global connection isn't defined or isn't set; attempting to reconnect..."
      begin
        $global_db_conn = Mysql::new(host, user, pass, name)
        conn = $global_db_conn
        
        if set_wait && defined?(DBWAIT)
          LOGGER.debug "Settign wait time for db connection to #{DBWAIT}"
          sql = "SET SESSION WAIT_TIMEOUT = #{DBWAIT};"  
          dbres = conn.query(sql)
        end

      rescue Mysql::Error => err
        LOGGER.error "Error making db connection: #{$1}"
        raise err
      end
    end
    
  # don't use global connection
  else
    LOGGER.debug "Not using global connection, creating anew..."
    # open new global connection  

    begin
      conn = Mysql::new(host, user, pass, name)
      
      if set_wait && defined?(DBWAIT)
        LOGGER.debug "Settign wait time for db connection to #{DBWAIT}"
        sql = "SET SESSION WAIT_TIMEOUT = #{DBWAIT};"  
        dbres = conn.query(sql)
      end
    rescue Mysql::Error => err
      LOGGER.error "Error making db connection: #{$1}"
      raise err
    end
  end
  
  conn
end

# close the database connection
def close_conn(conn)
  LOGGER.debug "Closing db connection"
  
  begin
    conn.close if conn
  rescue Mysql::Error => err
    LOGGER.error "Error closing db connection: #{$1}"
  end
    
end

def determine_log_level(level)
  case level
  when 1
    DEBUG
  when 2
    INFO
  when 3
    WARN
  when 4
    ERROR
  when 5
    FATAL
  else
    FATAL
  end
end

def parse_command_line_options
  options = CoolOptions.parse!("[options] <table name> <field name>") do |o|
    o.desc %q(Archive a database table in small bite size portions.
Note: Time zones are ignored in all dates and comparisons are based on >= start and < end, so the start is inclusive and ends is not.
New table schemas ignore the "extra" params.  This is because auto_increment fields could interfere with adding records and keeping referential integrity.
)
    o.on "db-host DATABASE_HOST",                     "Database host name"
    o.on "name DATABASE_NAME",                        "Database name"
    o.on "user DATABASE_USER",                        "Database user"
    o.on "password DATABASE_PASS",                    "Database password"
                
    o.on "default",                            "Start now and run for an hour (previous hours worth of records) (ignore start, end and lapse params)",    false
    o.on "start TIME",                         "Start time (mysql or epoch).  A lower time bound. ex 2007-10-01 16:00:00",                  ''
    o.on "end TIME",                           "End time (mysql or epoch). An upper time bound. ex 2007-10-01 17:00:00",                    ''
    o.on "lapse SECONDS",                      "Time lapse from start in seconds.",                                                         DEFAULT_TIME_LAPSE
    o.on "max-records NUMBER",                 "Number of records to copy. 0 = all",                                                        DEFAULT_MAX_RECORDS
    o.on "records-per-transaction NUMBER",     "Number of records to grab per transaction",                                                 DEFAULT_RECORDS_PER_TRANSACTION
    o.on "wait-time SECONDS",                  "Number of seconds to sleep between move transactions",                                      DEFAULT_SLEEP_TIME
    o.on "output-log-level NUMBER",            "Log level, 0-5 (off, debug, info, warn, error, fatal)",                                     DEFAULT_LOG_LEVEL
    o.on "table-name DEST_TABLE_NAME",         "Destination table name. Default table name is <table_name>_<datestamp>.",                   false

    o.after do |r|

      o.error("Error: Max records value must be an integer numeric value.") if (r.max_records =~ /^\d+$/).nil?            
      o.error("Error: Per transaction value must be an integer numeric value.") if (r.records_per_transaction =~ /^\d+$/).nil?      
      o.error("Error: Wait time must be an integer numeric value.") if (r.wait_time =~ /^\d+$/).nil?
      o.error("Error: Log level must be a numeric value 1-5") if (r.output_log_level =~ /^\d+$/).nil? || (r.output_log_level.to_i > 5) || (r.output_log_level.to_i < 0)
      
      r.max_records = r.max_records.to_i
      r.wait_time = r.wait_time.to_i
      r.records_per_transaction = r.records_per_transaction.to_i
      r.output_log_level = r.output_log_level.to_i
      
      # generally test purposes, these defaults could be changed:
      if r.default 
        r.end   = SCRIPT_RUN_TIME #now
        r.lapse = 60   # one minute
        r.start = SCRIPT_RUN_TIME - r.lapse

      # start and end were specified
      elsif (r.start != '' && r.end != '' && r.lapse == 0)
        r.start = DateTimeImport.convert_time(r.start)
        r.end = DateTimeImport.convert_time(r.end)      

        o.error("Error: Start date format is invalid.  For best results, use YYYY-MM-DD HH:MM:SS") if r.start.nil?
        o.error("Error: End date format is invalid.  For best results, use YYYY-MM-DD HH:MM:SS")  if r.end.nil?
        o.error("Error: Start date must be priaor to end date.")  if r.start >= r.end

        r.lapse = (r.end - r.start).to_i

      # if you aren't using the default you have to specify a start or end
      elsif (r.start != '' && r.end != '' && r.lapse != 0)  
        o.error("Error: Please specify either start/end, start/lapse, end/lapse options")

      elsif (r.start == '' && r.end == '')  
        o.error("Error: Start or end must be specified.")

      # we know the start and end both aren't empty, so check the other options
      # this means the start was specified and the lapse was nil
      elsif (r.lapse == 0 && r.start != '')
        o.error("Error: A lapse or end time must be specified")

      # this means the end was specified, but no start or lapse
      elsif (r.lapse == 0 && r.end != '')        
        o.error("Error: A lapse or start time must be specified")

      # using start and lapse
      elsif (r.start != '' && r.lapse != 0)
        o.error("Error: Lapse must be an integer value") if (r.lapse.to_s =~ /^[0-9]+$/).nil?

        r.lapse = r.lapse.to_i
        r.start = DateTimeImport.convert_time(r.start)
        r.end = (r.start + r.lapse)

        o.error("Error: Start date format is invalid.  For best results, use 'YYYY-MM-DD HH:MM:SS' or a unix time integer value.") if r.start.nil?
        o.error("Error: End date format is invalid.  For best results, use 'YYYY-MM-DD HH:MM:SS' or a unix time integer value.")  if r.end.nil?
        o.error("Error: Start date must be priaor to end date.")  if r.start >= r.end

      # using end and lapse
      elsif (r.end != '' && r.lapse != 0)
        o.error("Error: Lapse must be an integer value") if (r.lapse.to_s =~ /^[0-9]+$/).nil?

        r.lapse = r.lapse.to_i
        r.end = DateTimeImport.convert_time(r.end)
        r.start = r.end - r.lapse

        o.error("Error: Start date format is invalid.  For best results, use 'YYYY-MM-DD HH:MM:SS' or a unix time integer value.") if r.start.nil?
        o.error("Error: End date format is invalid.  For best results, use 'YYYY-MM-DD HH:MM:SS' or a unix time integer value.")  if r.end.nil?
        o.error("Error: Start date must be priaor to end date.")  if r.start >= r.end      
      end

    end
  end
  
  options
end

# ======================
# = Constants and more =
# ======================

# constants
DEFAULT_MAX_RECORDS = 0
DEFAULT_RECORDS_PER_TRANSACTION = 10
DEFAULT_TIME_LAPSE = 0
DEFAULT_SLEEP_TIME = 5
DEFAULT_LOG_LEVEL = 2

MYSQL_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
PRINTABLE_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
SECONDS_IN_WEEK = 604800
SCRIPT_RUN_TIME = Time.now
FIELD_NAME_TYPE = ['int', 'datetime']  # this is for the future, to automatically pull out the field name

# parse command line options
OPTIONS = parse_command_line_options()

TABLE_NAME = ARGV.shift || nil
NEW_TABLE_NAME =  OPTIONS.table_name.is_a?(String) ? OPTIONS.table_name : (!TABLE_NAME.nil? ? "#{TABLE_NAME}_#{SCRIPT_RUN_TIME.strftime("%Y%m%d")}" : nil)
FIELD_NAME = ARGV.shift || nil
START_TIME = OPTIONS.start
TIME_LAPSE = OPTIONS.lapse
END_TIME = OPTIONS.end
MAX_RECORDS = OPTIONS.max_records
ROWS_PER_TRANSACTION = OPTIONS.records_per_transaction
SLEEP_TIME = OPTIONS.wait_time
LOGGER = Logger.new('log')
LOGGER.level = determine_log_level(OPTIONS.output_log_level)
if OPTIONS.output_log_level > 0
  LOGGER.outputters = Outputter.stdout
end
  
DBHOST = OPTIONS.db_host
DBUSER = OPTIONS.user
DBPASS = OPTIONS.password
DBNAME = OPTIONS.name
DBWAIT = 43200

$global_db_conn = nil


# ========
# = Main =
# ========

if !TABLE_NAME.nil?
  LOGGER.info "Starting..."
  LOGGER.info "Records from #{START_TIME.strftime(PRINTABLE_DATETIME_FORMAT)} to #{END_TIME.strftime(PRINTABLE_DATETIME_FORMAT)} are being moved from #{TABLE_NAME} to #{NEW_TABLE_NAME}"
  LOGGER.info "This is a period of #{TIME_LAPSE} seconds, but could be interrupted at the limit of #{MAX_RECORDS} records"
  LOGGER.info "Note: Time zones are ignored and comparisons are based on >= start and < end, so the start is inclusive and ends is not."
  
  table_struct_hash = get_schema_struct(TABLE_NAME)
  create_new_table(get_schema_sql(table_struct_hash))
  field_data = {:name => FIELD_NAME, :min => START_TIME, :max => END_TIME, :type => get_field_datatype(table_struct_hash)}
  move_row_count = move_rows(field_data, table_struct_hash)
  close_conn($global_db_conn)
  LOGGER.info "Moved #{move_row_count} rows."
  LOGGER.info "You should probably run  OPTIMIZE TABLE `#{TABLE_NAME}`."
end