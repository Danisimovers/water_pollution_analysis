// ORM class for table 'high_pollution'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Thu Jun 19 18:36:10 MSK 2025
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

public class codegen_high_pollution extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        codegen_high_pollution.this.id = (Integer)value;
      }
    });
    setters.put("period", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        codegen_high_pollution.this.period = (java.sql.Date)value;
      }
    });
    setters.put("subject", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        codegen_high_pollution.this.subject = (String)value;
      }
    });
    setters.put("okato", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        codegen_high_pollution.this.okato = (String)value;
      }
    });
    setters.put("river_basin", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        codegen_high_pollution.this.river_basin = (String)value;
      }
    });
    setters.put("indicator", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        codegen_high_pollution.this.indicator = (String)value;
      }
    });
    setters.put("hazard_class", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        codegen_high_pollution.this.hazard_class = (Integer)value;
      }
    });
    setters.put("cnt_cases", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        codegen_high_pollution.this.cnt_cases = (Integer)value;
      }
    });
    setters.put("value_min", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        codegen_high_pollution.this.value_min = (java.math.BigDecimal)value;
      }
    });
    setters.put("value_max", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        codegen_high_pollution.this.value_max = (java.math.BigDecimal)value;
      }
    });
    setters.put("unit", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        codegen_high_pollution.this.unit = (String)value;
      }
    });
  }
  public codegen_high_pollution() {
    init0();
  }
  private Integer id;
  public Integer get_id() {
    return id;
  }
  public void set_id(Integer id) {
    this.id = id;
  }
  public codegen_high_pollution with_id(Integer id) {
    this.id = id;
    return this;
  }
  private java.sql.Date period;
  public java.sql.Date get_period() {
    return period;
  }
  public void set_period(java.sql.Date period) {
    this.period = period;
  }
  public codegen_high_pollution with_period(java.sql.Date period) {
    this.period = period;
    return this;
  }
  private String subject;
  public String get_subject() {
    return subject;
  }
  public void set_subject(String subject) {
    this.subject = subject;
  }
  public codegen_high_pollution with_subject(String subject) {
    this.subject = subject;
    return this;
  }
  private String okato;
  public String get_okato() {
    return okato;
  }
  public void set_okato(String okato) {
    this.okato = okato;
  }
  public codegen_high_pollution with_okato(String okato) {
    this.okato = okato;
    return this;
  }
  private String river_basin;
  public String get_river_basin() {
    return river_basin;
  }
  public void set_river_basin(String river_basin) {
    this.river_basin = river_basin;
  }
  public codegen_high_pollution with_river_basin(String river_basin) {
    this.river_basin = river_basin;
    return this;
  }
  private String indicator;
  public String get_indicator() {
    return indicator;
  }
  public void set_indicator(String indicator) {
    this.indicator = indicator;
  }
  public codegen_high_pollution with_indicator(String indicator) {
    this.indicator = indicator;
    return this;
  }
  private Integer hazard_class;
  public Integer get_hazard_class() {
    return hazard_class;
  }
  public void set_hazard_class(Integer hazard_class) {
    this.hazard_class = hazard_class;
  }
  public codegen_high_pollution with_hazard_class(Integer hazard_class) {
    this.hazard_class = hazard_class;
    return this;
  }
  private Integer cnt_cases;
  public Integer get_cnt_cases() {
    return cnt_cases;
  }
  public void set_cnt_cases(Integer cnt_cases) {
    this.cnt_cases = cnt_cases;
  }
  public codegen_high_pollution with_cnt_cases(Integer cnt_cases) {
    this.cnt_cases = cnt_cases;
    return this;
  }
  private java.math.BigDecimal value_min;
  public java.math.BigDecimal get_value_min() {
    return value_min;
  }
  public void set_value_min(java.math.BigDecimal value_min) {
    this.value_min = value_min;
  }
  public codegen_high_pollution with_value_min(java.math.BigDecimal value_min) {
    this.value_min = value_min;
    return this;
  }
  private java.math.BigDecimal value_max;
  public java.math.BigDecimal get_value_max() {
    return value_max;
  }
  public void set_value_max(java.math.BigDecimal value_max) {
    this.value_max = value_max;
  }
  public codegen_high_pollution with_value_max(java.math.BigDecimal value_max) {
    this.value_max = value_max;
    return this;
  }
  private String unit;
  public String get_unit() {
    return unit;
  }
  public void set_unit(String unit) {
    this.unit = unit;
  }
  public codegen_high_pollution with_unit(String unit) {
    this.unit = unit;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof codegen_high_pollution)) {
      return false;
    }
    codegen_high_pollution that = (codegen_high_pollution) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.period == null ? that.period == null : this.period.equals(that.period));
    equal = equal && (this.subject == null ? that.subject == null : this.subject.equals(that.subject));
    equal = equal && (this.okato == null ? that.okato == null : this.okato.equals(that.okato));
    equal = equal && (this.river_basin == null ? that.river_basin == null : this.river_basin.equals(that.river_basin));
    equal = equal && (this.indicator == null ? that.indicator == null : this.indicator.equals(that.indicator));
    equal = equal && (this.hazard_class == null ? that.hazard_class == null : this.hazard_class.equals(that.hazard_class));
    equal = equal && (this.cnt_cases == null ? that.cnt_cases == null : this.cnt_cases.equals(that.cnt_cases));
    equal = equal && (this.value_min == null ? that.value_min == null : this.value_min.equals(that.value_min));
    equal = equal && (this.value_max == null ? that.value_max == null : this.value_max.equals(that.value_max));
    equal = equal && (this.unit == null ? that.unit == null : this.unit.equals(that.unit));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof codegen_high_pollution)) {
      return false;
    }
    codegen_high_pollution that = (codegen_high_pollution) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.period == null ? that.period == null : this.period.equals(that.period));
    equal = equal && (this.subject == null ? that.subject == null : this.subject.equals(that.subject));
    equal = equal && (this.okato == null ? that.okato == null : this.okato.equals(that.okato));
    equal = equal && (this.river_basin == null ? that.river_basin == null : this.river_basin.equals(that.river_basin));
    equal = equal && (this.indicator == null ? that.indicator == null : this.indicator.equals(that.indicator));
    equal = equal && (this.hazard_class == null ? that.hazard_class == null : this.hazard_class.equals(that.hazard_class));
    equal = equal && (this.cnt_cases == null ? that.cnt_cases == null : this.cnt_cases.equals(that.cnt_cases));
    equal = equal && (this.value_min == null ? that.value_min == null : this.value_min.equals(that.value_min));
    equal = equal && (this.value_max == null ? that.value_max == null : this.value_max.equals(that.value_max));
    equal = equal && (this.unit == null ? that.unit == null : this.unit.equals(that.unit));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.period = JdbcWritableBridge.readDate(2, __dbResults);
    this.subject = JdbcWritableBridge.readString(3, __dbResults);
    this.okato = JdbcWritableBridge.readString(4, __dbResults);
    this.river_basin = JdbcWritableBridge.readString(5, __dbResults);
    this.indicator = JdbcWritableBridge.readString(6, __dbResults);
    this.hazard_class = JdbcWritableBridge.readInteger(7, __dbResults);
    this.cnt_cases = JdbcWritableBridge.readInteger(8, __dbResults);
    this.value_min = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.value_max = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.unit = JdbcWritableBridge.readString(11, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.period = JdbcWritableBridge.readDate(2, __dbResults);
    this.subject = JdbcWritableBridge.readString(3, __dbResults);
    this.okato = JdbcWritableBridge.readString(4, __dbResults);
    this.river_basin = JdbcWritableBridge.readString(5, __dbResults);
    this.indicator = JdbcWritableBridge.readString(6, __dbResults);
    this.hazard_class = JdbcWritableBridge.readInteger(7, __dbResults);
    this.cnt_cases = JdbcWritableBridge.readInteger(8, __dbResults);
    this.value_min = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.value_max = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.unit = JdbcWritableBridge.readString(11, __dbResults);
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
    JdbcWritableBridge.writeInteger(id, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeDate(period, 2 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeString(subject, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(okato, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(river_basin, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(indicator, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(hazard_class, 7 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(cnt_cases, 8 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(value_min, 9 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(value_max, 10 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(unit, 11 + __off, 12, __dbStmt);
    return 11;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(id, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeDate(period, 2 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeString(subject, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(okato, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(river_basin, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(indicator, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(hazard_class, 7 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(cnt_cases, 8 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(value_min, 9 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(value_max, 10 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(unit, 11 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.id = null;
    } else {
    this.id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.period = null;
    } else {
    this.period = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.subject = null;
    } else {
    this.subject = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.okato = null;
    } else {
    this.okato = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.river_basin = null;
    } else {
    this.river_basin = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.indicator = null;
    } else {
    this.indicator = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.hazard_class = null;
    } else {
    this.hazard_class = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.cnt_cases = null;
    } else {
    this.cnt_cases = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.value_min = null;
    } else {
    this.value_min = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.value_max = null;
    } else {
    this.value_max = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.unit = null;
    } else {
    this.unit = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.id);
    }
    if (null == this.period) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.period.getTime());
    }
    if (null == this.subject) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, subject);
    }
    if (null == this.okato) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, okato);
    }
    if (null == this.river_basin) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, river_basin);
    }
    if (null == this.indicator) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, indicator);
    }
    if (null == this.hazard_class) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.hazard_class);
    }
    if (null == this.cnt_cases) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.cnt_cases);
    }
    if (null == this.value_min) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.value_min, __dataOut);
    }
    if (null == this.value_max) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.value_max, __dataOut);
    }
    if (null == this.unit) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, unit);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.id);
    }
    if (null == this.period) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.period.getTime());
    }
    if (null == this.subject) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, subject);
    }
    if (null == this.okato) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, okato);
    }
    if (null == this.river_basin) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, river_basin);
    }
    if (null == this.indicator) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, indicator);
    }
    if (null == this.hazard_class) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.hazard_class);
    }
    if (null == this.cnt_cases) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.cnt_cases);
    }
    if (null == this.value_min) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.value_min, __dataOut);
    }
    if (null == this.value_max) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.value_max, __dataOut);
    }
    if (null == this.unit) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, unit);
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
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"null":"" + id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(period==null?"null":"" + period, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(subject==null?"null":subject, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(okato==null?"null":okato, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(river_basin==null?"null":river_basin, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(indicator==null?"null":indicator, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(hazard_class==null?"null":"" + hazard_class, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cnt_cases==null?"null":"" + cnt_cases, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(value_min==null?"null":value_min.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(value_max==null?"null":value_max.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(unit==null?"null":unit, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"null":"" + id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(period==null?"null":"" + period, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(subject==null?"null":subject, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(okato==null?"null":okato, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(river_basin==null?"null":river_basin, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(indicator==null?"null":indicator, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(hazard_class==null?"null":"" + hazard_class, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cnt_cases==null?"null":"" + cnt_cases, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(value_min==null?"null":value_min.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(value_max==null?"null":value_max.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(unit==null?"null":unit, delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
      this.id = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.period = null; } else {
      this.period = java.sql.Date.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.subject = null; } else {
      this.subject = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.okato = null; } else {
      this.okato = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.river_basin = null; } else {
      this.river_basin = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.indicator = null; } else {
      this.indicator = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.hazard_class = null; } else {
      this.hazard_class = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.cnt_cases = null; } else {
      this.cnt_cases = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.value_min = null; } else {
      this.value_min = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.value_max = null; } else {
      this.value_max = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.unit = null; } else {
      this.unit = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
      this.id = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.period = null; } else {
      this.period = java.sql.Date.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.subject = null; } else {
      this.subject = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.okato = null; } else {
      this.okato = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.river_basin = null; } else {
      this.river_basin = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.indicator = null; } else {
      this.indicator = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.hazard_class = null; } else {
      this.hazard_class = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.cnt_cases = null; } else {
      this.cnt_cases = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.value_min = null; } else {
      this.value_min = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.value_max = null; } else {
      this.value_max = new java.math.BigDecimal(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.unit = null; } else {
      this.unit = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    codegen_high_pollution o = (codegen_high_pollution) super.clone();
    o.period = (o.period != null) ? (java.sql.Date) o.period.clone() : null;
    return o;
  }

  public void clone0(codegen_high_pollution o) throws CloneNotSupportedException {
    o.period = (o.period != null) ? (java.sql.Date) o.period.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("period", this.period);
    __sqoop$field_map.put("subject", this.subject);
    __sqoop$field_map.put("okato", this.okato);
    __sqoop$field_map.put("river_basin", this.river_basin);
    __sqoop$field_map.put("indicator", this.indicator);
    __sqoop$field_map.put("hazard_class", this.hazard_class);
    __sqoop$field_map.put("cnt_cases", this.cnt_cases);
    __sqoop$field_map.put("value_min", this.value_min);
    __sqoop$field_map.put("value_max", this.value_max);
    __sqoop$field_map.put("unit", this.unit);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("period", this.period);
    __sqoop$field_map.put("subject", this.subject);
    __sqoop$field_map.put("okato", this.okato);
    __sqoop$field_map.put("river_basin", this.river_basin);
    __sqoop$field_map.put("indicator", this.indicator);
    __sqoop$field_map.put("hazard_class", this.hazard_class);
    __sqoop$field_map.put("cnt_cases", this.cnt_cases);
    __sqoop$field_map.put("value_min", this.value_min);
    __sqoop$field_map.put("value_max", this.value_max);
    __sqoop$field_map.put("unit", this.unit);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
