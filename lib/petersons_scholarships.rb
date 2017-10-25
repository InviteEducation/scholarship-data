require 'csv'

class PetersonsScholarships
  YEAR = 2017

  @@tables = {}

  def self.import
    ps = PetersonsScholarships.new
    ps.import_core
  end

  def self.tables
    @@tables
  end

  def self.parse_tables
    puts "Loading Peterson's scholarship side tables"
    @@tables.clear
    @@tables['fields_of_study'] = import_table 'CD1_ACAD.txt'
    @@tables['ethnicities'] = import_table 'CD4_ETHN.txt'
    @@tables['religious_affiliations'] = import_table 'CD5_RELG.txt'
  end

  def self.values(name)
    t = @@tables[name.to_s]
    t.nil? ? [] : t.values
  end

  def self.import_table(filename)
    hash = {}
    puts "Importing #{filename}"
    CSV.foreach("data/petersons_scholarships/#{filename}", col_sep: "\t",  headers: true, encoding: 'iso-8859-1:UTF-8') do |row|
      hash[row[0]] = row[1]
    end
    hash
  end

  def import_core
    puts "Loading Peterson's scholarship data"
    files = {
      "PA#{PetersonsScholarships::YEAR}.csv" => {
        petersons_id: string('id'),
        count_min: integer('loawds'),
        count_max: integer('hiawds'),
        amount_min: integer('loamnt'),
        amount_max: integer('hiamnt'),
        application_deadline: lambda do |row|
          month = string('apdlmo').call(row)
          if %w(C V).include?(month)
            month
          else
            month_day('apdlmo', 'apdlda').call(row)
          end
        end,
        renewable: yes_no('renyes', 'renno'),
        sponsor_url: string('sponsor-url'),

        high_school: yes_no('hs-yes', 'hs-no'),

        award_type: enum(scholarship: 'schol', loan: 'loan', prize: 'prize'),

        last_year_count: integer('numawd'),
        last_year_total: integer('totalawd'),

        school_types: enum_array(two_year: 'twoyr', four_year: 'fouryr', trade_or_tech: 'trtech'),

        military_services: enum_array(any: 'anysvc',
                               air_force: 'af',
                               army: 'army',
                               navy: 'navy',
                               marines: 'marines',
                               coast_guard: 'cg',
                               air_national_guard: 'airng',
                               army_national_guard: 'armyng',
                               special: 'kd'),

        student_type: enum(full_time: 'ft_only', part_time: 'pt_only', both: 'ft_pt'),

        residency_states: array((1..10), "st"),
        use_states: array((1..15), "sr-st"),

        fields_of_study: array((1..12), "m-stud", :fields_of_study),
        religious_affiliations: array((1..3), "rel", :religious_affiliations),
        ethnicities: array((1..5), "eth", :ethnicities),
        races: enum_array(american_indian: 'indesk', asian: 'asian', black: 'black', hispanic: 'hisp'),
        disabilities: enum_array(blind: 'blind', deaf: 'deaf', physical: 'phys', learning: 'learn')
      },

      "PA#{PetersonsScholarships::YEAR}_2.csv" => {
        petersons_id: string('id'),
        last_year_applications: integer('numapp'),
        postgrad: flag('postgrad'),
        award_type: enum(forgivable_loan: 'floan', grant: 'grant', fellowship: 'fellow'),
        citizenship_restrictions: array((0..5), "citizen"),
        citizenships_allowed: enum_array(us: 'usa_y', canadian: 'can_y', other: 'frn_y'),
        apply_url: string('appl_online_url')
      },

      "PA#{PetersonsScholarships::YEAR}_D.csv" => {
        petersons_id: string('id'),
        name: string('program_name'),
        description: string('desc'),
        donor_name: string('donor_name'),
        donor_description: string('donor_desc')
      }
    }.map{|filename, fields| {csv: CSV.open("data/petersons_scholarships/#{filename}", headers: true, encoding: 'iso-8859-1:UTF-8').each, fields: fields}}

    ids = Set.new
    begin
      while(true)
        hash = {}
        files.each do |spec|
          row = spec[:csv].next
          spec[:fields].each do |field, column|
            if field == :petersons_id && hash[field].present? && hash[field] != column.(row)
              puts 'CSV files are not sorted'
              abort 'Use must make sure that all csv files are sorted and they contain the same amount of rows for this rake task to work'
            end
            hash[field] ||= column.(row)
          end
        end
        ids << hash[:petersons_id]
        existing = Scholarship.where(petersons_id: hash[:petersons_id]).first
        if existing
          existing.update_attributes(hash)
        else
          Scholarship.create(hash)
        end
      end
    rescue StopIteration
      #i think this control flow mess will be fixed in Ruby 2.1, but in the meantime, this how you break out of enumerators
    end
    old_scholarships = Scholarship.where.not(petersons_id: ids.to_a)
    puts "Removing #{old_scholarships.count} scholarship(s) from the database"
    old_scholarships.destroy_all
  end

  def integer(field)
    val(field){|val| val.nil? ? nil : val.to_i}
  end

  def yes_no(yes_field, no_field)
    #could compose the fields, but this is probably less dumb
    lambda do |row|
      yes_val = row[yes_field.upcase]
      no_val = row[no_field.upcase]

      if yes_val == 'X'
        true
      elsif no_val == 'X'
        false
      else
        nil
      end
    end
  end

  def month_day(month_field, day_field)
    Petersons.month_day month_field.to_s.upcase, day_field.to_s.upcase
  end

  def flag(field)
    val(field){|val| val == 'X'}
  end

  def enum(map)
    lambda do |row|
      val = map.find{|field, col| row[col.upcase] == 'X'}
      val.nil? ? nil : val.first.to_s
    end
  end

  def array(range, string, translation = nil)
    lambda do |row|
      range.map do |i|
        val = row["#{string}#{i}".upcase]
        translation.nil? ? val : @@tables[translation.to_s][val]
      end.compact
    end
  end

  def enum_array(map)
    lambda do |row|
      map.map{|val, col| row[col.upcase] == 'X' ? val : nil}.compact
    end
  end

  def string(field)
    val(field){|val| val}
  end

  def val(field)
    lambda do |row|
      yield row[field.upcase]
    end
  end
end
