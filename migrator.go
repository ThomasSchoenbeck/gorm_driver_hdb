package gorm_driver_hdb

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

type Migrator struct {
	migrator.Migrator
	Dialector
}

func (m Migrator) FullDataTypeOf(field *schema.Field) clause.Expr {
	expr := m.Migrator.FullDataTypeOf(field)

	if value, ok := field.TagSettings["COMMENT"]; ok {
		expr.SQL += " COMMENT " + m.Dialector.Explain("?", value)
	}

	return expr
}

func (m Migrator) AlterColumn(value interface{}, field string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if field := stmt.Schema.LookUpField(field); field != nil {
			return m.DB.Exec(
				"ALTER TABLE ? MODIFY COLUMN ? ?",
				clause.Table{Name: stmt.Table}, clause.Column{Name: field.DBName}, m.FullDataTypeOf(field),
			).Error
		}
		return fmt.Errorf("failed to look up field with name: %s", field)
	})
}

func (m Migrator) RenameColumn(value interface{}, oldName, newName string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if m.Dialector.DontSupportRenameColumn {
			var field *schema.Field
			if f := stmt.Schema.LookUpField(oldName); f != nil {
				oldName = f.DBName
				field = f
			}

			if f := stmt.Schema.LookUpField(newName); f != nil {
				newName = f.DBName
				field = f
			}

			if field != nil {
				return m.DB.Exec(
					"ALTER TABLE ? CHANGE ? ? ?",
					clause.Table{Name: stmt.Table}, clause.Column{Name: oldName}, clause.Column{Name: newName}, m.FullDataTypeOf(field),
				).Error
			}
		} else {
			return m.Migrator.RenameColumn(value, oldName, newName)
		}

		return fmt.Errorf("failed to look up field with name: %s", newName)
	})
}

func (m Migrator) RenameIndex(value interface{}, oldName, newName string) error {
	if m.Dialector.DontSupportRenameIndex {
		return m.RunWithValue(value, func(stmt *gorm.Statement) error {
			err := m.DropIndex(value, oldName)
			if err == nil {
				if idx := stmt.Schema.LookIndex(newName); idx == nil {
					if idx = stmt.Schema.LookIndex(oldName); idx != nil {
						opts := m.BuildIndexOptions(idx.Fields, stmt)
						values := []interface{}{clause.Column{Name: newName}, clause.Table{Name: stmt.Table}, opts}

						createIndexSQL := "CREATE "
						if idx.Class != "" {
							createIndexSQL += idx.Class + " "
						}
						createIndexSQL += "INDEX ? ON ??"

						if idx.Type != "" {
							createIndexSQL += " USING " + idx.Type
						}

						return m.DB.Exec(createIndexSQL, values...).Error
					}
				}

				err = m.CreateIndex(value, newName)
			}

			return err
		})
	} else {
		return m.RunWithValue(value, func(stmt *gorm.Statement) error {
			return m.DB.Exec(
				"ALTER TABLE ? RENAME INDEX ? TO ?",
				clause.Table{Name: stmt.Table}, clause.Column{Name: oldName}, clause.Column{Name: newName},
			).Error
		})
	}
}

func (m Migrator) DropTable(values ...interface{}) error {
	values = m.ReorderModels(values, false)
	tx := m.DB.Session(&gorm.Session{})
	tx.Exec("SET FOREIGN_KEY_CHECKS = 0;")
	for i := len(values) - 1; i >= 0; i-- {
		if err := m.RunWithValue(values[i], func(stmt *gorm.Statement) error {
			return tx.Exec("DROP TABLE IF EXISTS ? CASCADE", clause.Table{Name: stmt.Table}).Error
		}); err != nil {
			return err
		}
	}
	tx.Exec("SET FOREIGN_KEY_CHECKS = 1;")
	return nil
}

func (m Migrator) DropConstraint(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		constraint, chk, table := m.GuessConstraintAndTable(stmt, name)
		if chk != nil {
			return m.DB.Exec("ALTER TABLE ? DROP CHECK ?", clause.Table{Name: stmt.Table}, clause.Column{Name: chk.Name}).Error
		}
		if constraint != nil {
			name = constraint.Name
		}

		return m.DB.Exec(
			"ALTER TABLE ? DROP FOREIGN KEY ?", clause.Table{Name: table}, clause.Column{Name: name},
		).Error
	})
}

// ColumnTypes column types return columnTypes,error
func (m Migrator) ColumnTypes(value interface{}) (columnTypes []gorm.ColumnType, err error) {
	columnTypes = make([]gorm.ColumnType, 0)
	err = m.RunWithValue(value, func(stmt *gorm.Statement) error {
		var (
			currentDatabase = m.DB.Migrator().CurrentDatabase()
			table           = stmt.Table
			columnTypeSQL   = `SELECT
			                      UPPER(COLUMN_NAME) as column_name
													, DEFAULT_VALUE as column_default
													, IS_NULLABLE as is_nullable
													, DATA_TYPE_NAME as data_type
													, LENGTH as character_maximum_length
													, CONCAT(DATA_TYPE_NAME, 
															CONCAT('(', 
																	CONCAT(LENGTH, 	
																			CONCAT( (CASE WHEN SCALE IS NOT NULL THEN CONCAT(',', SCALE) ELSE '' END),
																					')'
																			)
																	)
															)
													) as column_type
													, NULL as column_key
													, GENERATION_TYPE as extra
													, COMMENTS as column_comment
													, LENGTH as numeric_precision
													, SCALE as numeric_scale
			`
			rows, err = m.DB.Session(&gorm.Session{}).Table(table).Limit(1).Rows()
		)
		log.Println("currentDatabase", currentDatabase)
		log.Println("table", table)

		if err != nil {
			return err
		}

		rawColumnTypes, err := rows.ColumnTypes()

		if err := rows.Close(); err != nil {
			return err
		}

		if !m.DisableDatetimePrecision {
			columnTypeSQL += `, (CASE
				WHEN DATA_TYPE_NAME = 'DATE' THEN 10
				WHEN DATA_TYPE_NAME = 'TIME' THEN 12
				WHEN DATA_TYPE_NAME = 'SECONDDATE' THEN 10
				WHEN DATA_TYPE_NAME = 'TIMESTAMP' THEN 19
				ELSE NULL
				END
				) as datetime_precision `
		}
		columnTypeSQL += "FROM TABLE_COLUMNS WHERE SCHEMA_NAME = ? AND table_name = ?"

		columns, err := m.DB.Raw(columnTypeSQL, currentDatabase, stmt.Table).Rows()
		if err != nil {
			return err
		}
		defer columns.Close()

		for columns.Next() {
			var column migrator.ColumnType
			var datetimePrecision sql.NullInt64
			var extraValue sql.NullString
			var columnKey sql.NullString
			var values = []interface{}{&column.NameValue, &column.DefaultValueValue, &column.NullableValue, &column.DataTypeValue, &column.LengthValue, &column.ColumnTypeValue, &columnKey, &extraValue, &column.CommentValue, &column.DecimalSizeValue, &column.ScaleValue}

			if !m.DisableDatetimePrecision {
				values = append(values, &datetimePrecision)
			}

			if err = columns.Scan(values...); err != nil {
				return err
			}
			
			column.PrimaryKeyValue = sql.NullBool{Bool: false, Valid: true}
			column.UniqueValue = sql.NullBool{Bool: false, Valid: true}
			switch columnKey.String {
			case "PRI":
				column.PrimaryKeyValue = sql.NullBool{Bool: true, Valid: true}
			case "UNI":
				column.UniqueValue = sql.NullBool{Bool: true, Valid: true}
			}

			if strings.Contains(extraValue.String, "auto_increment") {
				column.AutoIncrementValue = sql.NullBool{Bool: true, Valid: true}
			}

			column.DefaultValueValue.String = strings.Trim(column.DefaultValueValue.String, "'")
			// if m.Dialector.DontSupportNullAsDefaultValue {
			// 	// rewrite mariadb default value like other version
			// 	if column.DefaultValueValue.Valid && column.DefaultValueValue.String == "NULL" {
			// 		column.DefaultValueValue.Valid = false
			// 		column.DefaultValueValue.String = ""
			// 	}
			// }

			if datetimePrecision.Valid {
				column.DecimalSizeValue = datetimePrecision
			}

			for _, c := range rawColumnTypes {
				if c.Name() == column.NameValue.String {
					column.SQLColumnType = c
					break
				}
			}

			columnTypes = append(columnTypes, column)
		}

		return nil
	})

	return columnTypes, err
}

func (m Migrator) CurrentSchema(stmt *gorm.Statement, table string) (string, string) {
	if strings.Contains(table, ".") {
		if tables := strings.Split(table, `.`); len(tables) == 2 {
			return tables[0], tables[1]
		}
	}

	return m.CurrentDatabase(), table
}
