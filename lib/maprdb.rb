# MapR-DB Client for JRuby
# This is a severely limited wrapper for the MapR-DB Java library. It was designed for Proof of Concept work, not production!

include Java
Dir["/opt/mapr/lib/\*.jar"].each { |jar| require jar }

java_import com.mapr.db.MapRDB
java_import com.mapr.db.Table
java_import org.ojai.Document
java_import org.ojai.DocumentStream
java_import org.ojai.store.DocumentMutation
java_import org.ojai.store.QueryCondition
java_import org.ojai.store.exceptions.DocumentExistsException

java_import org.ojai.types.ODate

java_import java.io.IOException
java_import java.util.Arrays
java_import java.util.Iterator

java_import org.ojai.store.QueryCondition

class MapR_DB


  # Creates a table if it doesn't already exist. Note: this will create the table in MapR-FS for your current user (/user/`whoami`/)
  def self.create_table_if_not_exists (table_name)
    table = nil
    if !MapRDB.table_exists?(table_name)
      table = MapRDB.create_table(table_name)
    end
  end

  # Inserts a document into the specified table
  def self.insert_into_table (table_name, document)
    if document.is_a? String
      document = MapRDB.new_document(document)
    end
    table = MapRDB.get_table(table_name)
    table.insert(document)
  end

  # Finds a document in the specified table by its ID
  def self.find_by_id (table_name, id)
    table = MapRDB.get_table(table_name)
    table.find_by_id(id)
  end

  # Creates a new connection to MapR-DB with the specified table
  def initialize (table_name)
    MapR_DB.create_table_if_not_exists(table_name)
    @table = MapRDB.get_table(table_name)
  end

  # Inserts a document into the current table
  def insert (document)
    if document.is_a? String
      document = MapRDB.new_document(document)
    end
    @table.insert(document)
  end

  # Inserts the document if its new, replaces it if it exists
  def insert_or_replace (document)
    if document.is_a? String
      document = MapRDB.new_document(document)
    end
    @table.insert_or_replace(document)
  end

  # Find a document by its ID
  def find_by_id (id)
    @table.find_by_id(id)
  end

end