# BulkImporter

This gem allows you to import a big amount of data from files to your database as quick as possible.

Supose you have a CSV file with 500k+ rows. Or even better: 2kk rows. Go, import it. Yes, I know, you can do it... right?. Now, do it in Rails... systematically :)
This task can be very upset and slow. You need a _bulk import_ operation. Thats is it. Thanks to BulkImporter you only need to write a few sentences and you'll get your data imported _and_ updated.

By now, we only support PostgreSQL 9.4+ databases thanks to the [COPY command](https://www.postgresql.org/docs/9.4/static/sql-copy.html).

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'bulk_importer'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install bulk_importer

## Usage

Suppose your CSV file _/tmp/names.csv_ (delimited by TABs) has the columns: _ID_ and _LOVELY_NAME_, and you need to import this information into your _names_ table, who has the fields _id_ and _name_.
Also, you want to keep your prexistent data but updated it if necessary.

```ruby
csv_file = File.new '/tmp/names.csv'

csv_columns = {
  'ID'          => id,
  'LOVELY_NAME' => name
}

# Suppose your table's key is the 'id' column.
keys = csv_columns.first

BulkImporter.import_from_csv(
  'names',
  csv_file,
  csv_columns,
  keys,
  delimiter: '\t',
  # UPDATE_MODE_UPDATE inserts the new data and update
  # preexistent (it search by keys).
  update_mode: BulkImportModule::UPDATE_MODE_UPDATE
)
```

Thats all! The imported data will be inserted in your _names_ table in a few seconds.


## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/abelosorio/bulk_importer.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).

## TO-DO

* Write more tests!
* Decouple database queries. This gem should work with any database engine. Yes, even MySQL.
* Since PostgreSQL 9.5 we could implement the UPDATE_MODE_UPDATE method by using [INSERT](https://www.postgresql.org/docs/9.5/static/sql-insert.html) command with the [ON CONFLICT](https://www.postgresql.org/docs/9.5/static/sql-insert.html#SQL-ON-CONFLICT) option.
* Support different database schemas.
