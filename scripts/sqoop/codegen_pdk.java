// ORM class for table 'pdk'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jun 21 16:11:42 MSK 2025
// For connector: org.apache.sqoop.manager.MySQLManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class pdk extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("ingredient", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        pdk.this.ingredient = (String)value;
      }
    });
    setters.put("ingredient_npa", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        pdk.this.ingredient_npa = (String)value;
      }
    });
    setters.put("hazard_class", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        pdk.this.hazard_class = (String)value;
      }
    });
    setters.put("value", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        pdk.this.value = (java.math.BigDecimal)value;
      }
    });
    setters.put("lpv", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        pdk.this.lpv = (String)value;
      }
    });
    setters.put("formula", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        pdk.this.formula = (String)value;
      }
    });
    setters.put("n_cas", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        pdk.this.n_cas = (String)value;
      }
    });
    setters.put("npa", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        pdk.this.npa = (String)value;
      }
    });
    setters.put("date_start", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        pdk.this.date_start = (java.sql.Date)value;
      }
    });
    setters.put("date_end", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        pdk.this.date_end = (java.sql.Date)value;
      }
    });
  }
  public pdk() {
    init0();
  }
  private String ingredient;
  public String get_ingredient() {
    return ingredient;
  }
  public void set_ingredient(String ingredient) {
    this.ingredient = ingredient;
  }
  public pdk with_ingredient(String ingredient) {
    this.ingredient = ingredient;
    return this;
  }
  private String ingredient_npa;
  public String get_ingredient_npa() {
    return ingredient_npa;
  }
  public void set_ingredient_npa(String ingredient_npa) {
    this.ingredient_npa = ingredient_npa;
  }
  public pdk with_ingredient_npa(String ingredient_npa) {
    this.ingredient_npa = ingredient_npa;
    return this;
  }
  private String hazard_class;
  public String get_hazard_class() {
    return hazard_class;
  }
  public void set_hazard_class(String hazard_class) {
    this.hazard_class = hazard_class;
  }
  public pdk with_hazard_class(String hazard_class) {
    this.hazard_class = hazard_class;
    return this;
  }
  private java.math.BigDecimal value;
  public java.math.BigDecimal get_value() {
    return value;
  }
  public void set_value(java.math.BigDecimal value) {
    this.value = value;
  }
  public pdk with_value(java.math.BigDecimal value) {
    this.value = value;
    return this;
  }
  private String lpv;
  public String get_lpv() {
    return lpv;
  }
  public void set_lpv(String lpv) {
    this.lpv = lpv;
  }
  public pdk with_lpv(String lpv) {
    this.lpv = lpv;
    return this;
  }
  private String formula;
  public String get_formula() {
    return formula;
  }
  public void set_formula(String formula) {
    this.formula = formula;
  }
  public pdk with_formula(String formula) {
    this.formula = formula;
    return this;
  }
  private String n_cas;
  public String get_n_cas() {
    return n_cas;
  }
  public void set_n_cas(String n_cas) {
    this.n_cas = n_cas;
  }
  public pdk with_n_cas(String n_cas) {
    this.n_cas = n_cas;
    return this;
  }
  private String npa;
  public String get_npa() {
    return npa;
  }
  public void set_npa(String npa) {
    this.npa = npa;
  }
  public pdk with_npa(String npa) {
    this.npa = npa;
    return this;
  }
  private java.sql.Date date_start;
  public java.sql.Date get_date_start() {
    return date_start;
  }
  public void set_date_start(java.sql.Date date_start) {
    this.date_start = date_start;
  }
  public pdk with_date_start(java.sql.Date date_start) {
    this.date_start = date_start;
    return this;
  }
  private java.sql.Date date_end;
  public java.sql.Date get_date_end() {
    return date_end;
  }
  public void set_date_end(java.sql.Date date_end) {
    this.date_end = date_end;
  }
  public pdk with_date_end(java.sql.Date date_end) {
    this.date_end = date_end;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof pdk)) {
      return false;
    }
    pdk that = (pdk) o;
    boolean equal = true;
    equal = equal && (this.ingredient == null ? that.ingredient == null : this.ingredient.equals(that.ingredient));
    equal = equal && (this.ingredient_npa == null ? that.ingredient_npa == null : this.ingredient_npa.equals(that.ingredient_npa));
    equal = equal && (this.hazard_class == null ? that.hazard_class == null : this.hazard_class.equals(that.hazard_class));
    equal = equal && (this.value == null ? that.value == null : this.value.equals(that.value));
    equal = equal && (this.lpv == null ? that.lpv == null : this.lpv.equals(that.lpv));
    equal = equal && (this.formula == null ? that.formula == null : this.formula.equals(that.formula));
    equal = equal && (this.n_cas == null ? that.n_cas == null : this.n_cas.equals(that.n_cas));
    equal = equal && (this.npa == null ? that.npa == null : this.npa.equals(that.npa));
    equal = equal && (this.date_start == null ? that.date_start == null : this.date_start.equals(that.date_start));
    equal = equal && (this.date_end == null ? that.date_end == null : this.date_end.equals(that.date_end));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof pdk)) {
      return false;
    }
    pdk that = (pdk) o;
    boolean equal = true;
    equal = equal && (this.ingredient == null ? that.ingredient == null : this.ingredient.equals(that.ingredient));
    equal = equal && (this.ingredient_npa == null ? that.ingredient_npa == null : this.ingredient_npa.equals(that.ingredient_npa));
    equal = equal && (this.hazard_class == null ? that.hazard_class == null : this.hazard_class.equals(that.hazard_class));
    equal = equal && (this.value == null ? that.value == null : this.value.equals(that.value));
    equal = equal && (this.lpv == null ? that.lpv == null : this.lpv.equals(that.lpv));
    equal = equal && (this.formula == null ? that.formula == null : this.formula.equals(that.formula));
    equal = equal && (this.n_cas == null ? that.n_cas == null : this.n_cas.equals(that.n_cas));
    equal = equal && (this.npa == null ? that.npa == null : this.npa.equals(that.npa));
    equal = equal && (this.date_start == null ? that.date_start == null : this.date_start.equals(that.date_start));
    equal = equal && (this.date_end == null ? that.date_end == null : this.date_end.equals(that.date_end));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.ingredient = JdbcWritableBridge.readString(1, __dbResults);
    this.ingredient_npa = JdbcWritableBridge.readString(2, __dbResults);
    this.hazard_class = JdbcWritableBridge.readString(3, __dbResults);
    this.value = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.lpv = JdbcWritableBridge.readString(5, __dbResults);
    this.formula = JdbcWritableBridge.readString(6, __dbResults);
    this.n_cas = JdbcWritableBridge.readString(7, __dbResults);
    this.npa = JdbcWritableBridge.readString(8, __dbResults);
    this.date_start = JdbcWritableBridge.readDate(9, __dbResults);
    this.date_end = JdbcWritableBridge.readDate(10, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.ingredient = JdbcWritableBridge.readString(1, __dbResults);
    this.ingredient_npa = JdbcWritableBridge.readString(2, __dbResults);
    this.hazard_class = JdbcWritableBridge.readString(3, __dbResults);
    this.value = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.lpv = JdbcWritableBridge.readString(5, __dbResults);
    this.formula = JdbcWritableBridge.readString(6, __dbResults);
    this.n_cas = JdbcWritableBridge.readString(7, __dbResults);
    this.npa = JdbcWritableBridge.readString(8, __dbResults);
    this.date_start = JdbcWritableBridge.readDate(9, __dbResults);
    this.date_end = JdbcWritableBridge.readDate(10, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(ingredient, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ingredient_npa, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(hazard_class, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(value, 4 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(lpv, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(formula, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(n_cas, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(npa, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDate(date_start, 9 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDate(date_end, 10 + __off, 91, __dbStmt);
    return 10;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(ingredient, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ingredient_npa, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(hazard_class, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(value, 4 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(lpv, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(formula, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(n_cas, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(npa, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDate(date_start, 9 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDate(date_end, 10 + __off, 91, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.ingredient = null;
    } else {
    this.ingredient = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ingredient_npa = null;
    } else {
    this.ingredient_npa = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.hazard_class = null;
    } else {
    this.hazard_class = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.value = null;
    } else {
    this.value = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.lpv = null;
    } else {
    this.lpv = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.formula = null;
    } else {
    this.formula = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.n_cas = null;
    } else {
    this.n_cas = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.npa = null;
    } else {
    this.npa = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.date_start = null;
    } else {
    this.date_start = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.date_end = null;
    } else {
    this.date_end = new Date(__dataIn.readLong());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.ingredient) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ingredient);
    }
    if (null == this.ingredient_npa) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ingredient_npa);
    }
    if (null == this.hazard_class) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, hazard_class);
    }
    if (null == this.value) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.value, __dataOut);
    }
    if (null == this.lpv) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, lpv);
    }
    if (null == this.formula) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, formula);
    }
    if (null == this.n_cas) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, n_cas);
    }
    if (null == this.npa) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, npa);
    }
    if (null == this.date_start) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.date_start.getTime());
    }
    if (null == this.date_end) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.date_end.getTime());
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.ingredient) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ingredient);
    }
    if (null == this.ingredient_npa) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ingredient_npa);
    }
    if (null == this.hazard_class) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, hazard_class);
    }
    if (null == this.value) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.value, __dataOut);
    }
    if (null == this.lpv) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, lpv);
    }
    if (null == this.formula) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, formula);
    }
    if (null == this.n_cas) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, n_cas);
    }
    if (null == this.npa) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, npa);
    }
    if (null == this.date_start) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.date_start.getTime());
    }
    if (null == this.date_end) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.date_end.getTime());
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(ingredient==null?"null":ingredient, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ingredient_npa==null?"null":ingredient_npa, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(hazard_class==null?"null":hazard_class, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(value==null?"null":value.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(lpv==null?"null":lpv, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(formula==null?"null":formula, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(n_cas==null?"null":n_cas, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(npa==null?"null":npa, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(date_start==null?"null":"" + date_start, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(date_end==null?"null":"" + date_end, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(ingredient==null?"null":ingredient, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ingredient_npa==null?"null":ingredient_npa, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(hazard_class==null?"null":hazard_class, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(value==null?"null":value.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(lpv==null?"null":lpv, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(formula==null?"null":formula, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(n_cas==null?"null":n_cas, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(npa==null?"null":npa, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(date_start==null?"null":"" + date_start, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(date_end==null?"null":"" + date_end, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.ingredient = null; } else {
      this.ingredient = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.ingredient_npa = null; } else {
      this.ingredient_npa = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.hazard_class = null; } else {
      this.hazard_class = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.value = null; } else {
      this.value = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.lpv = null; } else {
      this.lpv = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.formula = null; } else {
      this.formula = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.n_cas = null; } else {
      this.n_cas = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.npa = null; } else {
      this.npa = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.date_start = null; } else {
      this.date_start = java.sql.Date.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.date_end = null; } else {
      this.date_end = java.sql.Date.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.ingredient = null; } else {
      this.ingredient = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.ingredient_npa = null; } else {
      this.ingredient_npa = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.hazard_class = null; } else {
      this.hazard_class = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.value = null; } else {
      this.value = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.lpv = null; } else {
      this.lpv = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.formula = null; } else {
      this.formula = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.n_cas = null; } else {
      this.n_cas = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.npa = null; } else {
      this.npa = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.date_start = null; } else {
      this.date_start = java.sql.Date.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.date_end = null; } else {
      this.date_end = java.sql.Date.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    pdk o = (pdk) super.clone();
    o.date_start = (o.date_start != null) ? (java.sql.Date) o.date_start.clone() : null;
    o.date_end = (o.date_end != null) ? (java.sql.Date) o.date_end.clone() : null;
    return o;
  }

  public void clone0(pdk o) throws CloneNotSupportedException {
    o.date_start = (o.date_start != null) ? (java.sql.Date) o.date_start.clone() : null;
    o.date_end = (o.date_end != null) ? (java.sql.Date) o.date_end.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("ingredient", this.ingredient);
    __sqoop$field_map.put("ingredient_npa", this.ingredient_npa);
    __sqoop$field_map.put("hazard_class", this.hazard_class);
    __sqoop$field_map.put("value", this.value);
    __sqoop$field_map.put("lpv", this.lpv);
    __sqoop$field_map.put("formula", this.formula);
    __sqoop$field_map.put("n_cas", this.n_cas);
    __sqoop$field_map.put("npa", this.npa);
    __sqoop$field_map.put("date_start", this.date_start);
    __sqoop$field_map.put("date_end", this.date_end);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("ingredient", this.ingredient);
    __sqoop$field_map.put("ingredient_npa", this.ingredient_npa);
    __sqoop$field_map.put("hazard_class", this.hazard_class);
    __sqoop$field_map.put("value", this.value);
    __sqoop$field_map.put("lpv", this.lpv);
    __sqoop$field_map.put("formula", this.formula);
    __sqoop$field_map.put("n_cas", this.n_cas);
    __sqoop$field_map.put("npa", this.npa);
    __sqoop$field_map.put("date_start", this.date_start);
    __sqoop$field_map.put("date_end", this.date_end);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
