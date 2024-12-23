package opengauss

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"gitee.com/opengauss/openGauss-connector-go-pq"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

type Dialector struct {
	*Config
}

type Config struct {
	DriverName           string
	DSN                  string
	PreferSimpleProtocol bool
	WithoutReturning     bool
	Conn                 gorm.ConnPool
}

func Open(dsn string) gorm.Dialector {
	return &Dialector{&Config{DSN: dsn}}
}

func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

func (dia Dialector) Name() string {
	return "opengauss"
}

var timeZoneMatcher = regexp.MustCompile("(time_zone|TimeZone)=(.*?)($|&| )")

func (dia Dialector) Initialize(db *gorm.DB) (err error) {
	if dia.DriverName == "" {
		dia.DriverName = "opengauss"
	}

	callbackConfig := &callbacks.Config{
		CreateClauses: []string{"INSERT", "VALUES", "ON CONFLICT", "RETURNING"},
		UpdateClauses: []string{"UPDATE", "SET", "FROM", "WHERE", "RETURNING"},
		DeleteClauses: []string{"DELETE", "FROM", "WHERE", "RETURNING"},
	}
	// register callbacks
	callbacks.RegisterDefaultCallbacks(db, callbackConfig)

	if dia.Conn != nil {
		db.ConnPool = dia.Conn
	} else if dia.DriverName != "" {
		db.ConnPool, err = sql.Open(dia.DriverName, dia.Config.DSN)
	} else {
		// default use of driver openGauss-connector-go-pq
		var config *pq.Config
		config, err = pq.ParseConfig(dia.Config.DSN)
		if err != nil {
			return
		}
		result := timeZoneMatcher.FindStringSubmatch(dia.Config.DSN)
		if len(result) > 2 {
			config.RuntimeParams["timezone"] = result[2]
		}

		connector, _ := pq.NewConnectorConfig(config)
		db.ConnPool = sql.OpenDB(connector)

	}
	for k, v := range dia.clauseBuilders() {
		db.ClauseBuilders[k] = v
	}

	return
}

func (dia Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{migrator.Migrator{Config: migrator.Config{
		DB:                          db,
		Dialector:                   dia,
		CreateIndexAfterCreateTable: true,
	}}}
}

func (dia Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}

func (dia Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('$')
	writer.WriteString(strconv.Itoa(len(stmt.Vars)))
}

func (dia Dialector) QuoteTo(writer clause.Writer, str string) {
	var (
		underQuoted, selfQuoted bool
		continuousBacktick      int8
		shiftDelimiter          int8
	)

	for _, v := range []byte(str) {
		switch v {
		case '"':
			continuousBacktick++
			if continuousBacktick == 2 {
				writer.WriteString(`""`)
				continuousBacktick = 0
			}
		case '.':
			if continuousBacktick > 0 || !selfQuoted {
				shiftDelimiter = 0
				underQuoted = false
				continuousBacktick = 0
				writer.WriteByte('"')
			}
			writer.WriteByte(v)
			continue
		default:
			if shiftDelimiter-continuousBacktick <= 0 && !underQuoted {
				writer.WriteByte('"')
				underQuoted = true
				if selfQuoted = continuousBacktick > 0; selfQuoted {
					continuousBacktick -= 1
				}
			}

			for ; continuousBacktick > 0; continuousBacktick -= 1 {
				writer.WriteString(`""`)
			}

			writer.WriteByte(v)
		}
		shiftDelimiter++
	}

	if continuousBacktick > 0 && !selfQuoted {
		writer.WriteString(`""`)
	}
	writer.WriteByte('"')
}

var numericPlaceholder = regexp.MustCompile(`\$(\d+)`)

func (dia Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, numericPlaceholder, `'`, vars...)
}

func (dia Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "boolean"
	case schema.Int, schema.Uint:
		size := field.Size
		if field.DataType == schema.Uint {
			size++
		}
		if field.AutoIncrement {
			switch {
			case size <= 16:
				return "smallserial"
			case size <= 32:
				return "serial"
			default:
				return "bigserial"
			}
		} else {
			switch {
			case size <= 16:
				return "smallint"
			case size <= 32:
				return "integer"
			default:
				return "bigint"
			}
		}
	case schema.Float:
		if field.Precision > 0 {
			if field.Scale > 0 {
				return fmt.Sprintf("numeric(%d, %d)", field.Precision, field.Scale)
			}
			return fmt.Sprintf("numeric(%d)", field.Precision)
		}
		return "decimal"
	case schema.String:
		if field.Size > 0 {
			return fmt.Sprintf("varchar(%d)", field.Size)
		}
		return "text"
	case schema.Time:
		if field.Precision > 0 {
			return fmt.Sprintf("timestamptz(%d)", field.Precision)
		}
		return "timestamptz"
	case schema.Bytes:
		return "bytea"
	default:
		return dia.getSchemaCustomType(field)
	}
}

func (dia Dialector) getSchemaCustomType(field *schema.Field) string {
	sqlType := string(field.DataType)

	if field.AutoIncrement && !strings.Contains(strings.ToLower(sqlType), "serial") {
		size := field.Size
		if field.GORMDataType == schema.Uint {
			size++
		}
		switch {
		case size <= 16:
			sqlType = "smallserial"
		case size <= 32:
			sqlType = "serial"
		default:
			sqlType = "bigserial"
		}
	}

	return sqlType
}

func (dia Dialector) SavePoint(tx *gorm.DB, name string) error {
	tx.Exec("SAVEPOINT " + name)
	return nil
}

func (dia Dialector) RollbackTo(tx *gorm.DB, name string) error {
	tx.Exec("ROLLBACK TO SAVEPOINT " + name)
	return nil
}

func getSerialDatabaseType(s string) (dbType string, ok bool) {
	switch s {
	case "smallserial":
		return "smallint", true
	case "serial":
		return "integer", true
	case "bigserial":
		return "bigint", true
	default:
		return "", false
	}
}

func (Dialector) clauseBuilders() map[string]clause.ClauseBuilder {
	const onConflictKey, returningKey = "ON CONFLICT", "RETURNING"

	return map[string]clause.ClauseBuilder{
		onConflictKey: func(c clause.Clause, builder clause.Builder) {
			onConflict, _ := c.Expression.(clause.OnConflict)
			stmt := builder.(*gorm.Statement)
			s := stmt.Schema

			builder.WriteString("ON DUPLICATE KEY UPDATE ")

			firstColumn := true
			for idx, assignment := range onConflict.DoUpdates {
				lookUpField := s.LookUpField(assignment.Column.Name)
				tagSettings := lookUpField.TagSettings
				_, isUniqueIndex := tagSettings["UNIQUEINDEX"]
				// 'INSERT  ** ON DUPLICATE KEY UPDATE' don't allow update on primary key or unique key
				if lookUpField.Unique || lookUpField.PrimaryKey || isUniqueIndex {
					continue
				}

				if idx > 0 && !firstColumn {
					builder.WriteByte(',')
				}

				builder.WriteQuoted(assignment.Column)
				firstColumn = false
				builder.WriteByte('=')
				if column, ok := assignment.Value.(clause.Column); ok && column.Table == "excluded" {
					builder.WriteQuoted(column)
				} else {
					builder.AddVar(builder, assignment.Value)
				}
			}

			// add NOTHING
			if len(onConflict.DoUpdates) == 0 || onConflict.DoNothing == true || firstColumn {
				if s != nil {
					builder.WriteString("NOTHING ")
				}
			}

			// where condition
			if len(onConflict.TargetWhere.Exprs) > 0 {
				builder.WriteString(" WHERE ")
				onConflict.TargetWhere.Build(builder)
				builder.WriteByte(' ')
			}
		},
		returningKey: func(c clause.Clause, builder clause.Builder) {
			// exist bath 'RETURNING' and 'ON CONFLICT', 'RETURNING' clauses is invalid
			_, hasOnConflict := builder.(*gorm.Statement).Clauses[onConflictKey]
			if hasOnConflict {
				return
			}

			returning, _ := c.Expression.(clause.Returning)
			builder.WriteString("RETURNING ")
			if len(returning.Columns) > 0 {
				for idx, column := range returning.Columns {
					if idx > 0 {
						builder.WriteByte(',')
					}

					builder.WriteQuoted(column)
				}
			} else {
				builder.WriteByte('*')
			}
		},
	}
}
