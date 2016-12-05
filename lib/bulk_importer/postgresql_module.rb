# PostgreSQL module for Bulk importer.
#
# +see+ https://www.postgresql.org/docs/9.4/static/index.html
#
module PostgresqlModule
  # Copy from file.
  #
  # +see+ https://www.postgresql.org/docs/9.4/static/sql-copy.html
  #
  # ==== Parameters
  #
  # * +from+   path_to_some_file.
  # * +target+ Destination table name.
  #
  # ==== Options
  #
  # * +format+    File format. Defaults to 'csv'.
  # * +delimiter+ Column separator character. Defaults ','.
  # * +null+      String that represent null values. Defaults '' (empty).
  # * +header+    File includes header? Defaults to True.
  #
  # ==== Return
  #
  # +integer+ Number of imported rows.
  #
  def self.copy_from(from, target, format: 'csv', delimiter: ',', null: '', header: true)
    return -1 unless from.is_a? File

    if self.can_execute_copy
      # Copy file directly.
      result = ActiveRecord::Base.connection.execute self.make_import_sql(
        from.path,
        target,
        format:    format,
        delimiter: delimiter,
        null:      null,
        header:    header
      )
    else
      # Copy file by sending it trough stdin.
      result = self.copy_trough_stdin(
        from.path,
        target,
        format:    format,
        delimiter: delimiter,
        null:      null,
        header:    header
      )
    end

    result.cmd_tuples
  end

  # Copy from file by sendind it trough stdin.
  #
  # You should not run this method directly, instead use copy_from.
  #
  # ==== Parameters
  #
  # * +from+   path_to_some_file.
  # * +target+ Destination table name.
  #
  # ==== Options
  #
  # * +format+    File format. Defaults to 'csv'.
  # * +delimiter+ Column separator character. Defaults ','.
  # * +null+      String that represent null values. Defaults '' (empty).
  # * +header+    File includes header? Defaults to True.
  #
  # ==== Return
  #
  # +integer+ Number of imported rows.
  #
  def self.copy_trough_stdin(from, target, format: 'csv', delimiter: ',', null: '', header: true)

    # This solution was taken from:
    # http://www.kadrmasconcepts.com/blog/2013/12/15/copy-millions-of-rows-to-postgresql-with-rails

    conn = ActiveRecord::Base.connection
    raw  = conn.raw_connection

    raw.exec self.make_import_sql(
      'stdin',
      target,
      format:    format,
      delimiter: delimiter,
      null:      null,
      header:    header
    )

    file = File.open from, 'r'

    until file.eof?
      raw.put_copy_data file.readline
    end

    raw.put_copy_end
    file.close

    raw.get_result
  end

  # Get column types.
  #
  # ==== Parameters
  #
  # * +table+
  #
  # ==== Return
  #
  # +array+
  #
  def self.get_column_types(table)
    sql = self.make_get_column_types_sql(table)
    types = {}

    ActiveRecord::Base.connection.execute(sql).each do |row|
      types[row['name']] = row['type']
    end

    types
  end

  # Make the SQL statement of COPY FROM sentence
  #
  # ==== Options
  #
  # * +format+    File format. Defaults to 'csv'.
  # * +columns+   An optional array of columns to be copied. If no column list
  #               is specified, all columns of the table will be copied.
  # * +delimiter+ Column separator character. Defaults ','.
  # * +null+      String that represent null values. Defaults '' (empty).
  # * +header+    File includes header? Defaults to True.
  #
  # ==== Return
  #
  # +string+
  #
  def self.make_import_sql(from, target, format: 'csv', columns: [], delimiter: ',', null: '', header: true)
    sql = []

    sql << "COPY #{target}"
    sql << "(#{columns.join(',')})" unless columns.empty?
    sql << 'FROM'
    sql << (from.downcase == 'stdin' ? 'STDIN' : "'#{from}'")
    sql << self.make_import_options_sql(format, delimiter, null, header)

    sql.join ' '
  end

  # Makes the SQL options to COPY command.
  #
  # * +format+    File format. Defaults to 'csv'.
  # * +delimiter+ Column separator character. Defaults ','.
  # * +null+      String that represent null values. Defaults '' (empty).
  # * +header+    File includes header? Defaults to True.
  #
  # ==== Return
  #
  # +string+
  #
  def self.make_import_options_sql(format = 'csv', delimiter = ',', null = '', header = true)
    return nil if [ format, delimiter, null, header ].all? &:blank?

    options = []

    options << 'WITH'
    options << format.upcase unless format.blank?
    options << "DELIMITER E'#{delimiter}'" unless delimiter.blank?
    options << "NULL '#{null}'" unless null.blank?
    options << 'HEADER' if header == true and format.downcase == 'csv'

    options.join ' '
  end

  # Makes the SQL sentence to get column types. This should return two columns
  # at least: name, type.
  #
  # +see+ https://www.postgresql.org/docs/9.4/static/infoschema-columns.html
  #
  # ==== Parameters
  #
  # * +table+
  #
  # ==== Return
  #
  # +PG::Result+
  #
  # TODO: Add support to different schemas.
  #
  def self.make_get_column_types_sql(table)
    <<-eof
      SELECT column_name as name,
             udt_name as type
        FROM information_schema.columns
        WHERE table_name = '#{table}'
    eof
  end

  # Checks if the current db user can execute COPY.
  #
  # ==== Return
  #
  # +bool+
  #
  def self.can_execute_copy()
    # Only superusers can execute COPY.
    self.is_superuser
  end

  # Checks if the current user is SUPERUSER.
  #
  # ==== Return
  #
  # + bool+
  #
  def self.is_superuser
    query = <<-eof
      SELECT rolsuper
        FROM pg_roles
        WHERE rolname = CURRENT_USER
        LIMIT 1
    eof

    ActiveRecord::Base.connection.execute(query).first['rolsuper'] == 't'
  end

  # Create index on table (column(s)).
  #
  # * +table+
  # * +columns+
  #
  def self.create_index_on(table, columns)
    ActiveRecord::Base.connection.add_index table, columns
  end
end
