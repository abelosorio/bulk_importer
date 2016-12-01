require 'minitest/autorun'
require 'bulk_importer/postgresql_module.rb'

class PostgresqlModuleTest < MiniTest::Unit::TestCase
  def test_copy_sentences
    # Copy from CSV file, setting columns, and delimiter as ;
    assert_equal(
      "COPY foo (a,b,c) FROM 'bar' WITH CSV DELIMITER E';' HEADER",
      PostgresqlModule.make_import_sql(
        'bar', 'foo', format: 'csv', columns: [ 'a', 'b', 'c' ], delimiter: ';'
      )
    )

    # Copy from CSV, without columns or delimiter
    assert_equal(
      "COPY foo FROM 'bar' WITH CSV DELIMITER E',' HEADER",
      PostgresqlModule.make_import_sql('bar', 'foo', format: 'csv')
    )

    # Copy with all default values
    assert_equal(
      "COPY foo FROM 'bar' WITH CSV DELIMITER E',' HEADER",
      PostgresqlModule.make_import_sql('bar', 'foo')
    )
  end
end
