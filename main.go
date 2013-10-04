package main

import (
	"bufio"
	"encoding/json"
	"github.com/matthelm/sky.go"
	"launchpad.net/tomb"
	"log"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"
)

var (
	Filename   = "/Users/matthelm/Desktop/reports-tracking-app1.access.json.log.2"
	ScriptPath = "/Users/matthelm/Code/toronto_hack/read_from_file/sbin/tail_from_start.sh"
)

const (
	EventsChannelLength = 1024
	LinesChannelLength  = 2048
)

type Line struct {
	Id        string    `json:"event_id"`
	Referrer   string   `json:"referer"`
	Timestamp time.Time `json:"event_timestamp"`
	Uri       string    `json:"uri"`
}

type channelEvent struct {
	ObjectId string
	Event    *sky.Event
}

func main() {

	signalEnd := make(chan os.Signal, 1)
	signal.Notify(signalEnd, syscall.SIGINT)
	signal.Notify(signalEnd, syscall.SIGQUIT)
	signal.Notify(signalEnd, syscall.SIGTERM)

	client := sky.NewClient("localhost")
	deleteTable(client, "visits")
	table, _ := createTable(client, "visits")
	setupSchema(table)

	numCpus := runtime.NumCPU()
	log.Printf("Detected %d CPU(s)", numCpus)
	runtime.GOMAXPROCS(numCpus - 2)

	if _, err := os.Stat(Filename); os.IsNotExist(err) {
		log.Printf("Error getting follow file: no such file or directory: %s", Filename)
		return
	}

	// lines := make(chan string, LinesChannelLength)

	cmd := exec.Command(ScriptPath, strconv.FormatBool(true), Filename)
	cmdStdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Panicf("Error reading file with tail: %s", err)
	}
	cmdReader := bufio.NewReader(cmdStdout)
	cmd.Start()

	events := make(chan channelEvent, EventsChannelLength)

	var eventTomb tomb.Tomb
	go httpWriter(events, table, &eventTomb)

	for {
		line, err := cmdReader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading line from file: %s", err)
			eventTomb.Killf("Error reading line from file: %s", err)
			break
		}

		var j Line
		err = json.Unmarshal([]byte(line), &j)
		if err != nil {
			log.Printf("Error parsing JSON: %s", err)
		} else {
			argsUrl, _ := url.Parse(j.Uri)
			sessionMap := argsUrl.Query()
			sessionToken := sessionMap.Get("su")

			if sessionToken != "" && sessionToken != "undefined" {
				data := map[string]interface{}{
					"event_id": j.Id,
				}

				if j.Referrer != "" {
					data["referrer"] = j.Referrer
				}

				addPropertyToEvent(sessionMap, "v", "kind", data)
				addPropertyToEvent(sessionMap, "e", "engine", data)
				addPropertyToEvent(sessionMap, "q", "query", data)
				// addPropertyToEvent(sessionMap, "r", "referrer", data)
				addPropertyToEvent(sessionMap, "vi", "visit", data)
				addPropertyToEvent(sessionMap, "uq", "uniq", data)
				addPropertyToEvent(sessionMap, "su", "uniqToken", data)
				addPropertyToEvent(sessionMap, "sv", "visitToken", data)

				event := sky.NewEvent(j.Timestamp, data)
				channelEvent := channelEvent{sessionToken, event}

				log.Printf("Adding event to channel")
				events <- channelEvent
			}
		}
	}

	select {
	case <-signalEnd:
	case <-eventTomb.Dying():
	}

	eventTomb.Killf("Stopping the event writer")
	eventTomb.Wait()

	cmd.Process.Kill()
}

func addPropertyToEvent(sessionMap url.Values, key string, value string, data map[string]interface{}) {
	param := sessionMap.Get(key)
	if param != "" {
		data[value] = param
	}
}

func deleteTable(c sky.Client, tableName string) {
	table, err := c.GetTable(tableName)
	if err != nil {
		return
	}
	c.DeleteTable(table)
}

func createTable(c sky.Client, tableName string) (sky.Table, error) {
	newTable := sky.NewTable(tableName, c)
	c.CreateTable(newTable)
	return c.GetTable(tableName)
}

func setupSchema(t sky.Table) {
	createProperty("event_id", true, sky.String, t)
	createProperty("kind", true, sky.String, t)
	createProperty("engine", true, sky.String, t)
	createProperty("query", true, sky.String, t)
	createProperty("referrer", true, sky.String, t)
	createProperty("visit", true, sky.String, t)
	createProperty("uniq", true, sky.String, t)
	createProperty("uniqToken", true, sky.String, t)
	createProperty("visitToken", true, sky.String, t)
}

func createProperty(name string, transient bool, dataType string, t sky.Table) {
	property := sky.NewProperty(name, transient, dataType)
	t.CreateProperty(property)
}

func httpWriter(events chan channelEvent, table sky.Table, t *tomb.Tomb) {
	defer t.Done()

	function := func(eventStream *sky.EventStream) {
		for {
			select {
			case event := <-events:
				// log.Printf("Add event to Sky. %+v", event)
				eventStream.AddEvent(event.ObjectId, event.Event)
			case <-t.Dying():
				log.Printf("Events channel closed, terminating sends to that channel.")
				return
			default:
			}
		}
	}
	table.Stream(function)
}
