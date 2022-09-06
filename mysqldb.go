//Package mysqldb implements functions for dealing with the Delta database
package mysqldb

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"quadtrix.nl/audit"
	"quadtrix.nl/basicqueue"
	"quadtrix.nl/configmanager"
	"quadtrix.nl/servicelogger"
)

type DeltaMySQLLink struct {
	dblink           *sql.DB
	slog             *servicelogger.Logger
	cfg              *configmanager.Configuration
	monitorNotifier  *basicqueue.BasicQueue
	stopNotifier     *basicqueue.BasicQueue
	eventQueue       *basicqueue.BasicQueue
	asyncQueryQueue  *basicqueue.BasicQueue
	dbuser           string
	dbpass           string
	dbhost           string
	dbport           string
	dbname           string
	connected        bool
	queue_identifier string
	auditing         *audit.Audit
}

// New returns a new DeltaMySQLLink object
func New(slog *servicelogger.Logger, cfg *configmanager.Configuration, asyncQueryQueue *basicqueue.BasicQueue, monitorNotifier *basicqueue.BasicQueue, stopNotifier *basicqueue.BasicQueue, eventQueue *basicqueue.BasicQueue) (dmsl DeltaMySQLLink, err error) {
	dmsl.queue_identifier = fmt.Sprintf("mysqldb_%s", uuid.New().String())
	dmsl.dbuser = cfg.GetString("database.user")
	dmsl.dbpass = cfg.GetString("database.password")
	dmsl.dbhost = cfg.GetString("database.host")
	dmsl.dbport = cfg.GetString("database.port")
	dmsl.dbname = cfg.GetString("database.name")
	dmsl.monitorNotifier = monitorNotifier
	dmsl.stopNotifier = stopNotifier
	dmsl.eventQueue = eventQueue
	dmsl.asyncQueryQueue = asyncQueryQueue
	err = dmsl.monitorNotifier.RegisterConsumer(dmsl.queue_identifier)
	if err != nil {
		return DeltaMySQLLink{}, err
	}
	err = dmsl.stopNotifier.RegisterConsumer(dmsl.queue_identifier)
	if err != nil {
		return DeltaMySQLLink{}, err
	}
	err = dmsl.eventQueue.RegisterProducer(dmsl.queue_identifier)
	if err != nil {
		return DeltaMySQLLink{}, err
	}
	err = dmsl.eventQueue.RegisterConsumer(dmsl.queue_identifier)
	if err != nil {
		return DeltaMySQLLink{}, err
	}
	err = dmsl.asyncQueryQueue.RegisterConsumer(dmsl.queue_identifier)
	if err != nil {
		return DeltaMySQLLink{}, err
	}
	dmsl.connected = false
	dmsl.slog = slog
	dmsl.cfg = cfg
	dmsl.dblink, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dmsl.dbuser, dmsl.dbpass, dmsl.dbhost, dmsl.dbport, dmsl.dbname))
	//defer dmsl.dblink.Close()
	if err != nil {
		dmsl.slog.LogFatal("New", "mysqldb", fmt.Sprintf("Unable to open database connection to %s:%s/%s: %s", dmsl.dbhost, dmsl.dbport, dmsl.dbname, err.Error()), 20)
		return DeltaMySQLLink{}, err
	}

	var version string
	err = dmsl.dblink.QueryRow("SELECT VERSION()").Scan(&version)
	if err != nil {
		dmsl.slog.LogFatal("New", "mysqldb", fmt.Sprintf("Unable to determine server version: %s", err.Error()), 21)
		return DeltaMySQLLink{}, err
	}
	dmsl.slog.LogInfo("New", "mysqldb", fmt.Sprintf("Connected to %s at %s:%s/%s, username: %s", version, dmsl.dbhost, dmsl.dbport, dmsl.dbname, dmsl.dbuser))
	go dmsl.queuePolling()
	go dmsl.sendPing()
	dmsl.connected = true
	go dmsl.Stats()
	return dmsl, err
}

func (dmsl DeltaMySQLLink) checkOnReloadConfigMessage(message string) bool {
	messageparts := strings.Split(message, ":")
	if messageparts[0] == "CHECK_CONF" {
		dmsl.slog.LogTrace("checkOnReloadConfigMessage", "mysqldb", "Message is a CHECK_CONF message")
		return true
	}
	dmsl.slog.LogTrace("checkOnReloadConfigMessage", "mysqldb", "Message is not a CHECK_CONF message, ignoring")
	return false
}

func (dmsl *DeltaMySQLLink) checkReloadConfig() {
	dmsl.slog.LogDebug("checkReloadConfig", "mysqldb", "Config reload check triggered by queue message on queue.monitorNotification")
	dbuser := dmsl.cfg.GetString("database.user")
	dbpass := dmsl.cfg.GetString("database.password")
	dbhost := dmsl.cfg.GetString("database.host")
	dbport := dmsl.cfg.GetString("database.port")
	dbname := dmsl.cfg.GetString("database.name")
	if dmsl.dbuser != dbuser || dmsl.dbpass != dbpass || dmsl.dbhost != dbhost || dmsl.dbport != dbport || dmsl.dbname != dbname {
		dmsl.slog.LogInfo("checkReloadConfig", "mysqldb", "Database configuration has changed. Renewing database connection...")
		err := dmsl.dblink.Close()
		if err != nil {
			dmsl.slog.LogError("checkReloadConfig", "mysqldb", fmt.Sprintf("Error closing database connection: %s", err.Error()))
		}
		dmsl.connected = false
		dmsl.slog.LogDebug("checkReloadConfig", "mysqldb", fmt.Sprintf("Closed database connection to %s:%s/%s, username: %s", dmsl.dbhost, dmsl.dbport, dmsl.dbname, dmsl.dbuser))
		dmsl.dbuser = dbuser
		dmsl.dbpass = dbpass
		dmsl.dbhost = dbhost
		dmsl.dbport = dbport
		dmsl.dbname = dbname
		dmsl.slog.LogDebug("checkReloadConfig", "mysqldb", fmt.Sprintf("Connecting to %s:********@tcp(%s:%s)/%s", dbuser, dbhost, dbport, dbname))
		dmsl.dblink, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbuser, dbpass, dbhost, dbport, dbname))
		if err != nil {
			dmsl.slog.LogError("checkReloadConfig", "mysqldb", fmt.Sprintf("Error connecting to database at %s:%s, username: %s", dbhost, dbport, dbuser))
		}
		dmsl.connected = true
		defer dmsl.dblink.Close()
		var version string
		err = dmsl.dblink.QueryRow("SELECT VERSION()").Scan(&version)
		if err != nil {
			dmsl.slog.LogFatal("checkReloadConfig", "mysqldb", fmt.Sprintf("Unable to determine server version: %s", err.Error()), 22)
		}
		dmsl.slog.LogInfo("checkReloadConfig", "mysqldb", fmt.Sprintf("Connected to %s at %s:%s/%s, username: %s", version, dmsl.dbhost, dmsl.dbport, dmsl.dbname, dmsl.dbuser))
	} else {
		dmsl.slog.LogTrace("checkReloadConfig", "mysqldb", "Database configuration has not changed.")
	}
}

func (dmsl *DeltaMySQLLink) queuePolling() {
	dmsl.slog.LogInfo("queuePolling", "mysqldb", "Starting queue polling")
	var monitorHistory []string
	var stopHistory []string
	var eventHistory []string
	for {
		for dmsl.asyncQueryQueue.Poll(dmsl.queue_identifier) {
			message, err := dmsl.asyncQueryQueue.ReadJson(dmsl.queue_identifier)
			if err != nil {
				dmsl.slog.LogError("queuePolling.queue.asyncQueryQueue", "mysqldb", fmt.Sprintf("Error reading from queue: %s", err.Error()))
				continue
			}
			if message.MessageType == "QUERY" {
				err = dmsl.RunQueryInTransaction(message.Payload)
				if err != nil {
					dmsl.slog.LogError("RunQueryInTransaction", "mysqldb", fmt.Sprintf("Query failed: %s", err.Error()))
					continue
				}
			}
		}
		if dmsl.monitorNotifier.PollWithHistory(dmsl.queue_identifier, monitorHistory) {
			message, err := dmsl.monitorNotifier.ReadJsonWithHistory(dmsl.queue_identifier, monitorHistory)
			if err != nil {
				dmsl.slog.LogError("queuePolling.queue.monitorNotification", "mysqldb", fmt.Sprintf("Error reading from queue: %s", err.Error()))
				continue
			}
			monitorHistory = append(monitorHistory, message.MessageID)
			dmsl.slog.LogDebug("queuePolling.queue.monitorNotification", "mysqldb", fmt.Sprintf("Received monitor event from %s: %s: %s", message.Source, message.MessageType, message.Payload))
			if message.MessageType == "CHECK_CONF" {
				dmsl.slog.LogTrace("queuePolling.queue.monitorNotification", "mysqldb", "Received CHECK_CONF event. Waiting for 2 seconds to ensure the configuration is re-read")
				time.Sleep(2 * time.Second)
				if dmsl.checkOnReloadConfigMessage(message.Payload) {
					dmsl.checkReloadConfig()
				}
			}
		}
		if dmsl.stopNotifier.PollWithHistory(dmsl.queue_identifier, stopHistory) {
			message, err := dmsl.stopNotifier.ReadJsonWithHistory(dmsl.queue_identifier, stopHistory)
			if err != nil {
				dmsl.slog.LogError("queuePolling.queue.stopNotification", "mysqldb", fmt.Sprintf("Error reading from queue.stopNotification: %s", err.Error()))
			}
			stopHistory = append(stopHistory, message.MessageID)
			dmsl.slog.LogDebug("queuePolling.queue.stopNotification", "mysqldb", fmt.Sprintf("Received stop event from %s: %s", message.Source, message.MessageType))
			switch message.MessageType {
			case "STOPDB":
				dmsl.slog.LogInfo("queuePolling.queue.stopNotification", "mysqldb", fmt.Sprintf("Closing database connection to %s:%s/%s on queue.stopNotification trigger", dmsl.dbhost, dmsl.dbport, dmsl.dbname))
				dmsl.dblink.Close()
				dmsl.connected = false
			case "STOP":
				dmsl.slog.LogInfo("queuePolling.queue.stopNotification", "mysqldb", fmt.Sprintf("Closing database connection to %s:%s/%s, application stop event received", dmsl.dbhost, dmsl.dbport, dmsl.dbname))
				dmsl.dblink.Close()
				dmsl.connected = false
				dmsl.eventQueue.AddJsonMessage(dmsl.queue_identifier, "mysqldb", "main", "DBSTOPPED", "")
				break
			}

		}
		for dmsl.eventQueue.PollWithHistory(dmsl.queue_identifier, eventHistory) {
			message, err := dmsl.eventQueue.ReadJsonWithHistory(dmsl.queue_identifier, eventHistory)
			if err != nil {
				dmsl.slog.LogError("queuePolling.queue.events", "mysqldb", fmt.Sprintf("Error reading from queue: %s", err.Error()))
			}
			eventHistory = append(eventHistory, message.MessageID)
			if message.MessageType == "INITDB" {
				dmsl.slog.LogTrace("queuePolling.queue.events", "mysqldb", fmt.Sprintf("Received INITDB event from %s. Checking database state", message.Source))
				dmsl.checkDatabaseState(message.Payload)
			}
		}
		// Cleanup the histories
		if len(monitorHistory) > 50 {
			monitorHistory = monitorHistory[10:]
		}
		if len(stopHistory) > 50 {
			stopHistory = stopHistory[10:]
		}
		if len(eventHistory) > 50 {
			eventHistory = eventHistory[10:]
		}
		time.Sleep(time.Second)
	}
}

func (dmsl DeltaMySQLLink) patchDB(patch int) error {
	dmsl.slog.LogInfo(fmt.Sprintf("patchDB.%d", patch), "mysqldb", fmt.Sprintf("Applying database patch %d", patch))
	dbfile := fmt.Sprintf("%s/%d.sql", dmsl.cfg.GetString("database.patch_dir"), patch)
	filecontents, err := os.ReadFile(dbfile)
	if err != nil {
		return err
	}
	dmsl.slog.LogDebug(fmt.Sprintf("patchDB.%d", patch), "mysqldb", "Starting transaction")
	tx, err := dmsl.dblink.Begin()
	if err != nil {
		return err
	}
	queries := strings.Split(string(filecontents), ";")
	space := regexp.MustCompile(`\s+`)
	for _, query := range queries {
		flatquery := space.ReplaceAllString(query, " ")
		if dmsl.auditing != nil {
			firstword := strings.Split(flatquery, " ")[0]
			switch firstword {
			case "insert", "INSERT":
				dmsl.auditing.AuditLog(time.Now().Format("2006-01-02 15:04:05"), "mysqldb", "insert query", flatquery, audit.AU_SYSTEM)
			case "update", "UPDATE":
				dmsl.auditing.AuditLog(time.Now().Format("2006-01-02 15:04:05"), "mysqldb", "update query", flatquery, audit.AU_SYSTEM)
			case "delete", "DELETE":
				dmsl.auditing.AuditLog(time.Now().Format("2006-01-02 15:04:05"), "mysqldb", "delete query", flatquery, audit.AU_SYSTEM)
			case "create", "CREATE":
				dmsl.auditing.AuditLog(time.Now().Format("2006-01-02 15:04:05"), "mysqldb", "create query", flatquery, audit.AU_SYSTEM)
			case "alter", "ALTER":
				dmsl.auditing.AuditLog(time.Now().Format("2006-01-02 15:04:05"), "mysqldb", "alter query", flatquery, audit.AU_SYSTEM)
			case "drop", "DROP":
				dmsl.auditing.AuditLog(time.Now().Format("2006-01-02 15:04:05"), "mysqldb", "drop query", flatquery, audit.AU_SYSTEM)
			}
		}
		if len(flatquery) > 3 {
			dmsl.slog.LogTrace(fmt.Sprintf("patchDB.%d", patch), "mysqldb", fmt.Sprintf("%s", space.ReplaceAllString(query, " ")))
			statement, err := tx.Prepare(query)
			if err != nil {
				dmsl.slog.LogDebug(fmt.Sprintf("patchDB.%d", patch), "mysqldb", "Performing rollback on error")
				tx.Rollback()
				return err
			}
			result, err := statement.Exec()
			if err != nil {
				dmsl.slog.LogDebug(fmt.Sprintf("patchDB.%d", patch), "mysqldb", "Performing rollback on error")
				tx.Rollback()
				return err
			}
			rowsAff, err := result.RowsAffected()
			if err != nil {
				dmsl.slog.LogDebug(fmt.Sprintf("patchDB.%d", patch), "mysqldb", "Performing rollback on error")
				tx.Rollback()
				return err
			}
			dmsl.slog.LogTrace(fmt.Sprintf("patchDB.%d", patch), "mysqldb", fmt.Sprintf("Complete, %d rows affected", rowsAff))
		}
	}
	statement, err := tx.Prepare("INSERT INTO patches (dbversion, tstamp) VALUES(?, NOW())")
	if err != nil {
		dmsl.slog.LogDebug(fmt.Sprintf("patchDB.%d", patch), "mysqldb", "Performing rollback on error")
		tx.Rollback()
		return err
	}
	_, err = statement.Exec(patch)
	if err != nil {
		dmsl.slog.LogDebug(fmt.Sprintf("patchDB.%d", patch), "mysqldb", "Performing rollback on error")
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		dmsl.slog.LogDebug(fmt.Sprintf("patchDB.%d", patch), "mysqldb", fmt.Sprintf("Failed to commit transaction: %s", err.Error()))
		tx.Rollback()
		return err
	}
	dmsl.slog.LogDebug(fmt.Sprintf("patchDB.%d", patch), "mysqldb", fmt.Sprintf("Patching to version %d complete.", patch))
	return nil
}

func (dmsl DeltaMySQLLink) checkDatabaseState(version string) {
	versionnum, err := strconv.Atoi(version)
	fromVersion := 0
	if err != nil {
		dmsl.slog.LogFatal("checkDatabaseState", "mysqldb", fmt.Sprintf("Failed to read required database version: %s", err.Error()), 91)
		return
	}
	dmsl.slog.LogTrace("checkDatabaseState", "mysqldb", "SELECT dbversion FROM patches ORDER BY dbversion DESC LIMIT 1")
	rows, err := dmsl.dblink.Query("SELECT dbversion FROM patches ORDER BY dbversion DESC LIMIT 1")
	if err != nil {
		dmsl.slog.LogDebug("checkDatabaseState", "mysqldb", fmt.Sprintf("No patch version retrieved from database. Assuming empty."))
	} else {
		if rows.Next() {
			err = rows.Scan(&fromVersion)
			if err != nil {
				dmsl.slog.LogFatal("checkDatabaseState", "mysqldb", fmt.Sprintf("Failed to read database version from database: %s", err.Error()), 92)
				return
			}
			if fromVersion >= versionnum {
				dmsl.slog.LogInfo("checkDatabaseState", "mysqldb", fmt.Sprintf("Database is at version %d, version %d requested. Not patching", fromVersion, versionnum))
				return
			}
		}
	}
	dmsl.slog.LogInfo("checkDatabaseState", "mysqldb", fmt.Sprintf("Patching database from version %d to version %d", fromVersion, versionnum))
	for i := fromVersion + 1; i <= versionnum; i++ {
		err = dmsl.patchDB(i)
		if err != nil {
			dmsl.slog.LogFatal("checkDatabaseState", "mysqldb", fmt.Sprintf("Failed to patch database to version %d: %s", i, err.Error()), 93)
		}
	}
}

// IsConnected returns true when the DeltaMySQLLink object is connected to a database server
func (dmsl DeltaMySQLLink) IsConnected() bool {
	return dmsl.connected
}

// String returns a string representation of the DeltaMySQLLink object
func (dmsl DeltaMySQLLink) String() string {
	if !dmsl.IsConnected() {
		return "Not connected"
	} else {
		var version string
		err := dmsl.dblink.QueryRow("SELECT VERSION()").Scan(&version)
		if err != nil {
			return fmt.Sprintf("Unable to determine server version: %s", err.Error())
		}
		return fmt.Sprintf("Connected to %s at %s:%s/%s, username: %s", version, dmsl.dbhost, dmsl.dbport, dmsl.dbname, dmsl.dbuser)
	}
}

func (dmsl DeltaMySQLLink) Stats() {
	for {
		time.Sleep(time.Minute)
		dbstats := dmsl.dblink.Stats()
		dmsl.eventQueue.AddJsonMessage(dmsl.queue_identifier, "mysqldb", "main", "DBSTATS", fmt.Sprintf("%d;%d;%d;%d;%d;%d;%d;%d;%d", dbstats.Idle, dbstats.InUse, dbstats.MaxIdleClosed, dbstats.MaxIdleTimeClosed, dbstats.MaxLifetimeClosed, dbstats.MaxOpenConnections, dbstats.OpenConnections, dbstats.WaitCount, dbstats.WaitDuration.Milliseconds()))
	}
}

func (dmsl DeltaMySQLLink) sendPing() {
	for {
		time.Sleep(5 * time.Minute)
		dmsl.slog.LogTrace("sendPing", "mysqldb", "Pinging database")
		err := dmsl.dblink.Ping()
		if err != nil {
			dmsl.slog.LogError("sendnPing", "mysqldb", fmt.Sprintf("Pinging database failed: %s", err.Error()))
		}
	}
}

// RunQueryInTransaction runs a query in a transaction
func (dmsl DeltaMySQLLink) RunQueryInTransaction(query string) (err error) {
	dmsl.slog.LogTrace("RunQueryInTransaction", "mysqldb", query)
	if dmsl.auditing != nil {
		firstword := strings.Split(query, " ")[0]
		switch firstword {
		case "insert", "INSERT":
			dmsl.auditing.AuditLog(time.Now().Format("2006-01-02 15:04:05"), "mysqldb", "insert query", query, audit.AU_SYSTEM)
		case "update", "UPDATE":
			dmsl.auditing.AuditLog(time.Now().Format("2006-01-02 15:04:05"), "mysqldb", "update query", query, audit.AU_SYSTEM)
		case "delete", "DELETE":
			dmsl.auditing.AuditLog(time.Now().Format("2006-01-02 15:04:05"), "mysqldb", "delete query", query, audit.AU_SYSTEM)
		case "create", "CREATE":
			dmsl.auditing.AuditLog(time.Now().Format("2006-01-02 15:04:05"), "mysqldb", "create query", query, audit.AU_SYSTEM)
		case "alter", "ALTER":
			dmsl.auditing.AuditLog(time.Now().Format("2006-01-02 15:04:05"), "mysqldb", "alter query", query, audit.AU_SYSTEM)
		case "drop", "DROP":
			dmsl.auditing.AuditLog(time.Now().Format("2006-01-02 15:04:05"), "mysqldb", "drop query", query, audit.AU_SYSTEM)
		}
	}
	tx, err := dmsl.dblink.Begin()
	if err != nil {
		tx.Rollback()
		return err
	}
	statement, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return err
	}
	_, err = statement.Exec()
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}
	return nil
}

func (dmsl *DeltaMySQLLink) SetAuditing(au *audit.Audit) {
	dmsl.auditing = au
}

func (dmsl *DeltaMySQLLink) Select(fields string, table string, filters ...string) (rows *sql.Rows, err error) {
	filterstring := "WHERE "
	for _, filter := range filters {
		if filterstring == "WHERE " {
			filterstring = fmt.Sprintf("WHERE %s", filter)
		} else {
			filterstring = fmt.Sprintf("%s AND %s", filterstring, filter)
		}
	}
	if filterstring == "WHERE " {
		filterstring = ""
	}
	dmsl.slog.LogDebug("Select", "mysqldb", fmt.Sprintf("SELECT %s FROM %s %s", fields, table, filterstring))
	query := fmt.Sprintf("SELECT %s FROM %s %s", fields, table, filterstring)
	return dmsl.dblink.Query(query)
}
