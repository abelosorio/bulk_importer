require "bulk_importer/version"
require "active_support/all"
require "bulk_importer/postgresql_module.rb"

module BulkImporter
  # Update modes
  UPDATE_MODE_APPEND  = 'append'
  UPDATE_MODE_UPDATE  = 'update'
  UPDATE_MODE_REPLACE = 'replace'

  # Module name (used for loggin).
  NAME = 'BulkImporter'

  # Import data from a CSV file to an existing table
  #
  # ==== Parameters
  #
  # * +target+  Target table.
  # * +file+    Source CSV file.
  # * +columns+ Array of CSV columns.
  # * +keys+    Primary keys of destination table.
  #
  # ==== Options
  #
  # * +delimiter+
  # * +null+
  # * +header+
  # * +update_mode+ Update mode for imported data.
  #
  # ==== Updated modes
  #
  # * +self::UPDATE_MODE_APPEND+  Move only new data (default).
  # * +self::UPDATE_MODE_UPDATE+  Insert new data and updated prexistent.
  # * +self::UPDATE_MODE_REPLACE+ Truncate old data and insert the new one.
  #
  # ==== Return
  #
  # +integer+ Number of imported rows.
  #
  def self.import_from_csv(target, file, columns, keys, delimiter: ',', null: '', header: true, update_mode: UPDATE_MODE_APPEND)
    return -1 unless file.is_a? File

    conn = ActiveRecord::Base.connection
    temp_name = target + '_' + Time.now.to_i.to_s + '_temporal'

    begin
      # Create temporary table (with all CSV fields)
      Rails.logger.debug \
        "[#{NAME}] Creating temporary table #{temp_name}(#{columns.keys})"
      conn.execute self.make_create_temp_table_sql(temp_name, columns.keys)

      # Import data
      Rails.logger.debug \
        "[#{NAME}] Importing data from #{file} to #{temp_name}"
      PostgresqlModule.copy_from(
        file,
        temp_name,
        format:    'csv',
        delimiter: delimiter,
        null:      null,
        header:    header
      )

      # Move data from temporary table to target and return total imported rows
      Rails.logger.debug \
        "[#{NAME}] Moving new data to #{target} with mode #{update_mode}"
      self.move_imported_data(temp_name, target, columns, keys, update_mode)
    rescue Exception => e
      Rails.logger.error e.message
      Rails.logger.error e.backtrace
      return -1
    ensure
      # Drop temporary table (if exists)
      ActiveRecord::Base.connection.execute "DROP TABLE IF EXISTS #{temp_name}"
    end
  end

  # Move imported data from origin (raw imported) to destination.
  #
  # ==== Parameters
  #
  # * +origin+      Origin (temporary table).
  # * +destination+ Destination table.
  # * +columns+     Array of CSV columns.
  # * +keys+        Primary keys of destination table.
  # * +update_mode+ Update mode.
  #
  # ==== Return
  #
  # +integer+ Number of imported rows.
  #
  def self.move_imported_data(origin, destination, columns, keys, update_mode)
    unless self.is_update_mode_valid update_mode
      raise "[#{NAME}] Unknown update mode: #{update_mode}"
    end

    # Create an index to improve move performance.
    Rails.logger.debug "[#{NAME}] Creating index on #{origin}"
    PostgresqlModule.create_index_on origin, keys.keys.map { |i| i.downcase }

    queries = self.make_move_imported_data_sql(
      origin,
      destination,
      columns,
      keys,
      update_mode
    )

    rows = 0

    queries.each do |query|
      Rails.logger.debug "[#{NAME}] Running query <<#{query}>>"
      rows += ActiveRecord::Base.connection.execute(query).cmd_tuples
    end

    return rows
  end

  # Makes the SQL command to move the imported data.
  #
  # ==== Parameters
  #
  # * +origin+      Origin (temporary table).
  # * +destination+ Destination table.
  # * +columns+     Array of CSV columns.
  # * +keys+        Primary keys of destination table.
  # * +update_mode+ Update mode.
  #
  # ==== Return
  #
  # +array+ Array of queries to execute.
  #
  def self.make_move_imported_data_sql(origin, destination, columns, keys, update_mode)
    case update_mode
    when UPDATE_MODE_APPEND
      # Insert new
      self.make_update_mode_append_sql origin, destination, columns, keys
    when UPDATE_MODE_UPDATE
      # Insert new and Update prexistent
      self.make_update_mode_update_sql origin, destination, columns, keys
    when UPDATE_MODE_REPLACE
      # Truncate destination and Insert new (all)
      self.make_update_mode_replace_sql origin, destination, columns, keys
    end
  end

  # Makes the SQL command to append new imported data.
  #
  # ==== Parameters
  #
  # * +origin+      Origin (temporary table).
  # * +destination+ Destination table.
  # * +columns+     Array of CSV columns.
  # * +keys+        Primary keys of destination table.
  #
  # ==== Return
  #
  # +array+ Array of queries to execute
  #
  def self.make_update_mode_append_sql(origin, destination, columns, keys)
    sql = []

    columns = columns.delete_if { |item| columns[item].nil? }
    types = {}

    pg_types = PostgresqlModule.get_column_types destination
    columns.values.each { |i| types[columns.invert[i]] = pg_types[i] }

    sql << "INSERT INTO #{destination}"
    sql << "(#{columns.values.join(',')})"
    sql << "SELECT #{self.keys_to_list(columns.keys, 'o', types)}"
    sql << "FROM #{origin} o"
    sql << "LEFT JOIN #{destination} d"
    sql << "ON (#{self.keys_to_list(keys.keys, 'o', types)}) = "
    sql << "(#{self.keys_to_list(keys.values, 'd')})"
    sql << "WHERE (#{self.keys_to_list(keys.values, 'd')}) is null"

    [ sql.join(' ') ]
  end

  # Makes the SQL command to update prexistent data.
  #
  # ==== Parameters
  #
  # * +origin+      Origin (temporary table).
  # * +destination+ Destination table.
  # * +columns+     Array of CSV columns.
  # * +keys+        Primary keys of destination table.
  #
  # ==== Return
  #
  # +array+ Array of queries to execute
  #
  def self.make_update_mode_update_sql(origin, destination, columns, keys)
    q = self.make_update_mode_append_sql(origin, destination, columns, keys)

    sql = []
    types = {}

    pg_types = PostgresqlModule.get_column_types destination
    columns.values.each { |i| types[columns.invert[i]] = pg_types[i] }

    o_columns_without_keys = columns.keys.delete_if { |i| keys.has_key? i }
    d_columns_without_keys = columns.values.delete_if { |i| keys.has_value? i }

    set = []
    columns.delete_if { |item| columns[item].nil? }.keys.each do |column|
      set << "#{columns[column]} = o.#{column}::#{types[column]}"
    end
    sets = set.join(',')

    if columns.has_value? 'updated_at'
      # Field is updated if origin's updated_at is greater than destination.

      PostgresqlModule.create_index_on origin, columns.invert['updated_at'].downcase

      sql << "UPDATE #{destination} d SET #{sets}"
      sql << "FROM #{origin} o"
      sql << "WHERE (#{self.keys_to_list(keys.keys, 'o', types)}) = "
      sql << "(#{self.keys_to_list(keys.values, 'd')}) AND"
      sql << "o.#{columns.invert['updated_at'].downcase}::timestamp > d.updated_at"
    else
      # Check if any field changed
      sql << "WITH #{origin}_prexistent_modified AS ("
      sql << "SELECT o.* FROM #{origin} o JOIN #{destination} d"
      sql << "ON (#{self.keys_to_list(keys.keys, 'o', types)}) = "
      sql << "(#{self.keys_to_list(keys.values, 'd')}) AND "
      sql << "(#{self.keys_to_list(o_columns_without_keys, 'o', types)}) != "
      sql << "(#{self.keys_to_list(d_columns_without_keys, 'd')})"
      sql << ")"
      sql << "UPDATE #{destination} d SET #{sets}"
      sql << "FROM #{origin}_prexistent_modified o"
      sql << "WHERE (#{self.keys_to_list(keys.keys, 'o', types)}) = "
      sql << "(#{self.keys_to_list(keys.values, 'd')})"
    end

    q << sql.join(' ')

    return q
  end

  # Makes the SQL command to remove existing data and Insert new (all).
  #
  # ==== Parameters
  #
  # * +origin+      Origin (temporary table).
  # * +destination+ Destination table.
  # * +columns+     Array of CSV columns.
  # * +keys+        Primary keys of destination table.
  #
  # ==== Return
  #
  # +array+ Array of queries to execute
  #
  def self.make_update_mode_replace_sql(origin, destination, columns, key)
    q = []

    q << "TRUNCATE TABLE #{destination}"
    q.concat self.make_update_mode_append_sql origin, destination, columns, keys

    return q
  end

  # Makes the SQL command to create a temporary table.
  #
  # ==== Parameters
  #
  # * +name+    Name of temporary table.
  # * +columns+ Array of columns (just name).
  #
  # ==== Return
  #
  # +string+
  #
  def self.make_create_temp_table_sql(name, columns)
    columns = columns.map { |i| i + ' text' }
    "CREATE TEMPORARY TABLE #{name} (#{columns.join(',')})"
  end

  # Translate an array of keys in a list with an optional prefix.
  #
  # ==== Parameters
  #
  # * +keys+
  # * +prefix+
  # * +types+
  #
  # ==== Return
  #
  # * +string+
  #
  # TODO: Add doc
  #
  def self.keys_to_list(keys, prefix = nil, types = nil)
    list = []

    keys.each do |i|
      col = i
      col = [ prefix, i ].compact.join('.') unless prefix.nil?
      col = [ col, types[i] ].compact.join('::') unless types.nil?

      list << col
    end

    list.join ','
  end

  # Check if the update mode is valid.
  #
  # ==== Parameters
  #
  # * +update_mode+
  #
  # ==== Return
  #
  # +bool+
  #
  def self.is_update_mode_valid(update_mode)
    valid_update_modes = []

    self.constants.each do |constant|
      if constant.to_s.start_with? 'UPDATE_MODE_'
        valid_update_modes << self.const_get(constant)
      end
    end

    valid_update_modes.include? update_mode.to_s
  end
end
