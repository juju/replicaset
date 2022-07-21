// Copyright 2013-2015 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

// Package replicaset provides convenience functions and structures for
// creating and managing MongoDB replica sets via the mgo driver.
package replicaset

import (
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/juju/collections/set"
	"github.com/juju/errors"
	"github.com/juju/loggo"
	"github.com/juju/mgo/v3"
	"github.com/juju/mgo/v3/bson"
	"github.com/juju/utils/v3"
)

const (
	// maxInitiateAttempts is the maximum number of times to attempt
	// replSetInitiate for each call to Initiate.
	maxInitiateAttempts = 10

	// initiateAttemptDelay is the amount of time to sleep between failed
	// attempts to replSetInitiate.
	initiateAttemptDelay = 100 * time.Millisecond

	// maxInitiateStatusAttempts is the maximum number of attempts
	// to get the replication status after Initiate.
	maxInitiateStatusAttempts = 50

	// initiateAttemptStatusDelay is the amount of time to sleep between failed
	// attempts to replSetGetStatus.
	initiateAttemptStatusDelay = 500 * time.Millisecond
)

var logger = loggo.GetLogger("juju.replicaset")

var (
	getCurrentStatus = CurrentStatus
	getBuildInfo     = BuildInfo
	isReady          = IsReady
	attemptInitiate  = doAttemptInitiate
)

// doAttemptInitiate will attempt to initiate a mongodb replicaset with each of
// the given configs, returning as soon as one config is successful.
func doAttemptInitiate(monotonicSession *mgo.Session, cfg []Config) error {
	var err error
	for _, c := range cfg {
		logger.Infof("Initiating replicaset with config: %s", fmtConfigForLog(&c))
		if err = monotonicSession.Run(bson.D{{"replSetInitiate", c}}, nil); err != nil {
			logger.Infof("Unsuccessful attempt to initiate replicaset: %v", err)
			continue
		}
		return nil
	}
	return err
}

// Initiate sets up a replica set with the given replica set name with the
// single given member.  It need be called only once for a given mongo replica
// set.  The tags specified will be added as tags on the member that is created
// in the replica set.
//
// Note that you must set DialWithInfo and set Direct = true when dialing into a
// specific non-initiated mongo server.
//
// See http://docs.mongodb.org/manual/reference/method/rs.initiate/ for more
// details.
func Initiate(session *mgo.Session, address, name string, tags map[string]string) error {
	monotonicSession := session.Clone()
	defer monotonicSession.Close()
	monotonicSession.SetMode(mgo.Monotonic, true)

	// For mongo 4 and above, we use protocol version 1.
	buildInfo, err := getBuildInfo(monotonicSession)
	if err != nil {
		return err
	}
	protocolVersion := 0
	if buildInfo.VersionAtLeast(4) {
		protocolVersion = 1
	}

	// We don't know mongod's ability to use a correct IPv6 addr format
	// until the server is started, but we need to know before we can start
	// it. Try the older, incorrect format, if the correct format fails.
	cfg := []Config{
		{
			Name:            name,
			Version:         1,
			ProtocolVersion: protocolVersion,
			Members: []Member{{
				Id:      1,
				Address: address,
				Tags:    tags,
			}},
		}, {
			Name:            name,
			Version:         1,
			ProtocolVersion: protocolVersion,
			Members: []Member{{
				Id:      1,
				Address: formatIPv6AddressWithoutBrackets(address),
				Tags:    tags,
			}},
		},
	}

	// Attempt replSetInitiate, with potential retries.
	for i := 0; i < maxInitiateAttempts; i++ {
		monotonicSession.Refresh()
		if err = attemptInitiate(monotonicSession, cfg); err != nil {
			time.Sleep(initiateAttemptDelay)
			continue
		}
		break
	}

	// Wait for replSetInitiate to complete. Even if err != nil,
	// it may be that replSetInitiate is still in progress, so
	// attempt CurrentStatus.
	for i := 0; i < maxInitiateStatusAttempts; i++ {
		monotonicSession.Refresh()
		var status *Status
		status, err = getCurrentStatus(monotonicSession)
		if err != nil {
			logger.Warningf("Initiate: fetching replication status failed: %v", err)
		}
		if err != nil || len(status.Members) == 0 {
			time.Sleep(initiateAttemptStatusDelay)
			continue
		}
		break
	}
	return err
}

// Member holds configuration information for a replica set member.
//
// See http://docs.mongodb.org/manual/reference/replica-configuration/
// for more details
type Member struct {
	// Id is a unique id for a member in a set.
	Id int `bson:"_id"`

	// Address holds the network address of the member,
	// in the form hostname:port.
	Address string `bson:"host"`

	// Arbiter holds whether the member is an arbiter only.
	// This value is optional; it defaults to false.
	Arbiter *bool `bson:"arbiterOnly,omitempty"`

	// BuildIndexes determines whether the mongod builds indexes on this member.
	// This value is optional; it defaults to true.
	BuildIndexes *bool `bson:"buildIndexes,omitempty"`

	// Hidden determines whether the replica set hides this member from
	// the output of IsMaster.
	// This value is optional; it defaults to false.
	Hidden *bool `bson:"hidden,omitempty"`

	// Priority determines eligibility of a member to become primary.
	// This value is optional; it defaults to 1.
	Priority *float64 `bson:"priority,omitempty"`

	// Tags store additional information about a replica member, often used for
	// customizing read preferences and write concern.
	Tags map[string]string `bson:"tags,omitempty"`

	// SlaveDelay describes the number of seconds behind the master that this
	// replica set member should lag rounded up to the nearest second.
	// This value is optional; it defaults to 0.
	SlaveDelay *time.Duration `bson:"slaveDelay,omitempty"`

	// Votes controls the number of votes a server has in a replica set election.
	// This value is optional; it defaults to 1.
	Votes *int `bson:"votes,omitempty"`
}

// fmtConfigForLog generates a succinct string suitable for debugging.
func fmtConfigForLog(config *Config) string {
	return fmt.Sprintf(`{
  Name: %s,
  Version: %d,
  Term: %d,
  Protocol Version: %d,
  Members: {
%s
  },
}`, config.Name, config.Version, config.Term, config.ProtocolVersion, fmtMembersForLog(config.Members))
}

// fmtMembersForLog generates a succinct string suitable for debugging what the Members are up to.
// Note that Members will be printed in Id sorted order, regardless of the order in membersToLog.
func fmtMembersForLog(membersToLog []Member) string {
	memberInfo := make([]string, len(membersToLog))
	members := append([]Member(nil), membersToLog...)
	sort.SliceStable(members, func(i, j int) bool { return members[i].Id < members[j].Id })
	for i, member := range members {
		voting := "not-voting"
		if member.Votes == nil || *member.Votes > 0 {
			voting = "voting"
		}
		var tags []string
		for key, val := range member.Tags {
			tags = append(tags, fmt.Sprintf("%s:%s", key, val))
		}
		memberInfo[i] = fmt.Sprintf("    {%d %q %v %s},", member.Id, member.Address, strings.Join(tags, ", "), voting)
	}
	return strings.Join(memberInfo, "\n")
}

// fmtUpdatedMembersForLog generates a succinct string suitable for debugging what the Members are up to.
// Note that Members will be printed in Id sorted order, regardless of the order in membersToLog.
func fmtUpdatedMembersForLog(membersToLog []Member) string {
	memberInfo := make([]string, len(membersToLog))
	members := append([]Member(nil), membersToLog...)
	sort.SliceStable(members, func(i, j int) bool { return members[i].Id < members[j].Id })
	for i, member := range members {
		voting := "not-voting"
		if member.Votes == nil || *member.Votes > 0 {
			voting = "voting"
		}
		var tags []string
		for key, val := range member.Tags {
			tags = append(tags, fmt.Sprintf("%s:%s", key, val))
		}
		memberInfo[i] = fmt.Sprintf("    {%d %q %v %s},", member.Id, member.Address, strings.Join(tags, ", "), voting)
	}
	return strings.Join(memberInfo, "\n")
}

// applyReplSetConfigChanges applies the specified changes to the mongo session.
// It also logs what the changes are.
func applyReplSetConfigChanges(cmd string, session mgoSession, currentConfig *Config, updated []Member, added []Member, removed []int) error {
	logger.Debugf("%s() changing replica set\n%s\n- updated:\n%#v\n- added:\n%s\n- removed: %v",
		cmd, fmtConfigForLog(currentConfig), fmtUpdatedMembersForLog(updated), fmtMembersForLog(added), removed)

	if currentConfig == nil {
		return errors.New("current config cannot be nil")
	}
	newConfig := *currentConfig
	// Mongo 4.4 onwards requires we process one change at a time.
	// First do the updates.
	for _, u := range updated {
		haveChanges := false
		for n := range newConfig.Members {
			haveChanges = true
			newConfig.Members[n] = u
			break
		}
		if !haveChanges {
			return fmt.Errorf("cannot update member %#v in replicaset, no existing member", u)
		}
		newConfig.Version++
		err := applyReplSetConfig(cmd, session, newConfig)
		if err != nil {
			return fmt.Errorf("cannot update member %#v in replicaset: %v", u, err)
		}
	}
	// Then the adds.
	for _, m := range added {
		newConfig.Version++
		newConfig.Members = append(newConfig.Members, m)
		err := applyReplSetConfig(cmd, session, newConfig)
		if err != nil {
			return fmt.Errorf("cannot add member %#v to replicaset: %v", m, err)
		}
	}

	// Then do the removes.
	for _, id := range removed {
		haveChanges := false
		for n, m := range newConfig.Members {
			if m.Id != id {
				continue
			}
			haveChanges = true
			newConfig.Members = append(newConfig.Members[:n], newConfig.Members[n+1:]...)
			break
		}
		if !haveChanges {
			continue
		}
		newConfig.Version++
		err := applyReplSetConfig(cmd, session, newConfig)
		if err != nil {
			return fmt.Errorf("cannot remove member %d from replicaset: %v", id, err)
		}
	}
	return nil
}

type mgoSession interface {
	// These are mocked for testing.

	Run(cmd interface{}, result interface{}) error
	Ping() error
	Refresh()

	// These are called by a function which is patched.

	Clone() *mgo.Session
	Close()
	SetMode(consistency mgo.Mode, refresh bool)
	DB(name string) *mgo.Database
}

// applyReplSetConfig applies the specified config to the mongo session.
// It checks if the replica set changes cause the DB connection to be dropped.
// If so, it Refreshes the session and tries to Ping again.
func applyReplSetConfig(cmd string, session mgoSession, newConfig Config) error {
	err := session.Run(bson.D{{"replSetReconfig", newConfig}}, nil)
	if err == io.EOF {
		// If the primary changes due to replSetReconfig, then all
		// current connections are dropped.
		// Refreshing should fix us up.
		logger.Debugf("got EOF while running %s(), calling session.Refresh()", cmd)
		session.Refresh()
	} else if err != nil {
		// For all errors that aren't EOF, return immediately
		return err
	}
	err = nil
	// We will only try to Ping 2 times
	for i := 0; i < 2; i++ {
		// err was either nil, or EOF and we called Refresh, so Ping to
		// make sure we're actually connected
		err = session.Ping()
		if err == nil {
			break
		}
	}
	return err
}

var localHostIpv4 = regexp.MustCompile(`127\.0\.0\.\d+`)

func isLocalhost(addr string) bool {
	return addr == "::1" ||
		addr == "0:0:0:0:0:0:0:1" ||
		localHostIpv4.MatchString(addr) ||
		addr == "localhost"
}

// Add adds the given members to the session's replica set.  Duplicates of
// existing replicas will be ignored.
//
// Members will have their Ids set automatically if they are not already > 0
func Add(session *mgo.Session, members ...Member) error {
	config, err := CurrentConfig(session)
	if err != nil {
		return err
	}

	max := findMaxId(config.Members, members)

	var added []Member
outerLoop:
	for _, newMember := range members {
		for _, member := range config.Members {
			if member.Address == newMember.Address ||
				isLocalhost(member.Address) && isLocalhost(newMember.Address) {
				// already exists, skip it
				continue outerLoop
			}
		}
		// let the caller specify an id if they want, treat zero as unspecified
		if newMember.Id < 1 {
			max++
			newMember.Id = max
		}
		added = append(added, newMember)
	}
	return applyReplSetConfigChanges("Add", session, config, nil, added, nil)
}

// Remove removes members with the given addresses from the replica set. It is
// not an error to remove addresses of non-existent replica set members.
func Remove(session *mgo.Session, addrs ...string) error {
	config, err := CurrentConfig(session)
	if err != nil {
		return err
	}
	var toRemove []int
	for _, rem := range addrs {
		for _, repl := range config.Members {
			if repl.Address == rem ||
				isLocalhost(repl.Address) && isLocalhost(rem) {
				toRemove = append(toRemove, repl.Id)
				break
			}
		}
	}
	return applyReplSetConfigChanges("Remove", session, config, nil, nil, toRemove)
}

// findMaxId looks through both sets of members and makes sure we cannot reuse an Id value
func findMaxId(oldMembers, newMembers []Member) int {
	max := 0
	for _, m := range oldMembers {
		if m.Id > max {
			max = m.Id
		}
	}
	// Also check if any of the members being passed in already have an Id that we would be reusing.
	for _, m := range newMembers {
		if m.Id > max {
			max = m.Id
		}
	}
	return max
}

// Set changes the current set of replica set members.  Members will have their
// ids set automatically if their ids are not already > 0.
func Set(session mgoSession, members []Member) error {
	config, err := CurrentConfig(session)
	if err != nil {
		return err
	}

	// Assign ids to members that did not previously exist, starting above the
	// value of the highest id that already existed.
	existingAddressIds := map[string]int{}
	existingMembers := map[int]Member{}
	max := findMaxId(config.Members, members)
	for _, m := range config.Members {
		existingAddressIds[m.Address] = m.Id
		existingMembers[m.Id] = m
	}

	// Gather the wanted ids so we can see which ones have been removed.
	wantIds := map[string]int{}
	for _, m := range members {
		wantIds[m.Address] = m.Id
	}

	// Compose the added and updated members.
	var (
		added   []Member
		updated []Member
		one     = 1.0
		votes   = 1
	)
	updatedIds := set.NewInts()
	for _, m := range members {
		existingMember, ok := existingMembers[m.Id]
		if ok {
			if existingMember.Address != m.Address {
				updatedIds.Add(m.Id)
				updated = append(updated, m)
			}
			continue
		}
		if m.Id < 1 {
			max++
			m.Id = max
		}
		m.Priority = &one
		m.Votes = &votes
		added = append(added, m)
	}

	// Remove all the wanted addresses from the existing ones.
	// Any remaining are those to be removed.
	for addr := range wantIds {
		delete(existingAddressIds, addr)
	}
	var removedIds []int
	for _, id := range existingAddressIds {
		if !updatedIds.Contains(id) {
			removedIds = append(removedIds, id)
		}
	}

	return applyReplSetConfigChanges("Set", session, config, updated, added, removedIds)
}

// IsMasterResults holds information about the configuration of a given mongo node.
type IsMasterResults struct {
	// The following fields hold information about the specific mongodb node.
	IsMaster  bool      `bson:"ismaster"`
	Secondary bool      `bson:"secondary"`
	Arbiter   bool      `bson:"arbiterOnly"`
	Address   string    `bson:"me"`
	LocalTime time.Time `bson:"localTime"`

	// The following fields hold information about the replica set.
	ReplicaSetName string   `bson:"setName"`
	Addresses      []string `bson:"hosts"`
	Arbiters       []string `bson:"arbiters"`
	PrimaryAddress string   `bson:"primary"`
}

// IsMaster returns information about the configuration of the node that
// the given session is connected to.
func IsMaster(session *mgo.Session) (*IsMasterResults, error) {
	results := &IsMasterResults{}
	err := session.Run("isMaster", results)
	if err != nil {
		return nil, err
	}

	results.Address = formatIPv6AddressWithBrackets(results.Address)
	results.PrimaryAddress = formatIPv6AddressWithBrackets(results.PrimaryAddress)
	for index, address := range results.Addresses {
		results.Addresses[index] = formatIPv6AddressWithBrackets(address)
	}
	return results, nil
}

var ErrMasterNotConfigured = fmt.Errorf("mongo master not configured")

// MasterHostPort returns the "address:port" string for the primary
// mongo server in the replicaset. It returns ErrMasterNotConfigured if
// the replica set has not yet been initiated.
func MasterHostPort(session *mgo.Session) (string, error) {
	results, err := IsMaster(session)
	if err != nil {
		return "", err
	}
	if results.PrimaryAddress == "" {
		return "", ErrMasterNotConfigured
	}
	return results.PrimaryAddress, nil
}

// CurrentMembers returns the current members of the replica set.
func CurrentMembers(session *mgo.Session) ([]Member, error) {
	cfg, err := CurrentConfig(session)
	if err != nil {
		return nil, err
	}
	return cfg.Members, nil
}

// CurrentConfig returns the Config for the given session's replica set.  If
// there is no current config, the error returned will be mgo.ErrNotFound.
var CurrentConfig = currentConfig

func currentConfig(session mgoSession) (*Config, error) {
	cfg := &Config{}
	monotonicSession := session.Clone()
	defer monotonicSession.Close()
	monotonicSession.SetMode(mgo.Monotonic, true)
	err := monotonicSession.DB("local").C("system.replset").Find(nil).One(cfg)
	if err == mgo.ErrNotFound {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("cannot get replset config: %s", err.Error())
	}

	members := make([]Member, len(cfg.Members), len(cfg.Members))
	for index, member := range cfg.Members {
		member.Address = formatIPv6AddressWithBrackets(member.Address)
		members[index] = member
	}
	// Sort the values by Member.Id
	sort.Slice(members, func(i, j int) bool { return members[i].Id < members[j].Id })
	cfg.Members = members
	logger.Debugf("current replicaset config: %s", fmtConfigForLog(cfg))
	return cfg, nil
}

// Config is the document stored in mongodb that defines the servers in the
// replica set
type Config struct {
	Name            string   `bson:"_id"`
	Version         int      `bson:"version"`
	Term            int      `bson:"term,omitempty"`
	ProtocolVersion int      `bson:"protocolVersion,omitempty"`
	Members         []Member `bson:"members"`
}

// StepDownPrimary asks the current mongo primary to step down.
// Note that triggering a step down causes all client connections to be
// disconnected. We explicitly treat the io.EOF we get as not being an error,
// but all other sessions will also be disconnected.
func StepDownPrimary(session *mgo.Session) error {
	strictSession := session.Clone()
	defer strictSession.Close()
	// StepDown can only be called on the primary
	session.SetMode(mgo.Primary, true)
	// replSetStepDown takes a few optional parameters that vary based on what
	// version of Mongo is running. In Mongo 2.4 it just takes the "step down
	// seconds" which claims to default to 60s.
	// In 3.2 it can also take secondaryCatchUpPeriodSecs which is supposed to
	// start at 10s. However, testing shows that not passing either gives:
	// err{"stepdown period must be longer than secondaryCatchUpPeriodSecs"}
	err := session.Run(bson.D{{"replSetStepDown", 60.0}}, nil)
	// we expect to get io.EOF so don't treat it as a failure.
	if err == io.EOF {
		return nil
	}
	return err
}

// BuildInfo returns the mongod build info for the given session.
func BuildInfo(session *mgo.Session) (mgo.BuildInfo, error) {
	return session.BuildInfo()
}

// CurrentStatus returns the status of the replica set for the given session.
func CurrentStatus(session *mgo.Session) (*Status, error) {
	status := &Status{}
	err := session.Run("replSetGetStatus", status)
	if err != nil {
		return nil, fmt.Errorf("cannot get replica set status: %v", err)
	}

	for index, member := range status.Members {
		status.Members[index].Address = formatIPv6AddressWithBrackets(member.Address)
	}
	return status, nil
}

// Status holds data about the status of members of the replica set returned
// from replSetGetStatus
//
// See http://docs.mongodb.org/manual/reference/command/replSetGetStatus/#dbcmd.replSetGetStatus
type Status struct {
	Name    string         `bson:"set"`
	Members []MemberStatus `bson:"members"`
}

// MemberStatus holds the status of a replica set member returned from
// replSetGetStatus.
type MemberStatus struct {
	// Id holds the replica set id of the member that the status is describing.
	Id int `bson:"_id"`

	// Address holds address of the member that the status is describing.
	Address string `bson:"name"`

	// Self holds whether this is the status for the member that
	// the session is connected to.
	Self bool `bson:"self"`

	// ErrMsg holds the most recent error or status message received
	// from the member.
	ErrMsg string `bson:"errmsg"`

	// Healthy reports whether the member is up. It is true for the
	// member that the request was made to.
	Healthy bool `bson:"health"`

	// State describes the current state of the member.
	State MemberState `bson:"state"`

	// Uptime describes how long the member has been online.
	Uptime time.Duration `bson:"uptime"`

	// Ping describes the length of time a round-trip packet takes to travel
	// between the remote member and the local instance.  It is zero for the
	// member that the session is connected to.
	Ping time.Duration `bson:"pingMS"`
}

// IsReady checks on the status of all members in the replicaset
// associated with the provided session. If we can connect and the majority of
// members are ready then the result is true.
func IsReady(session *mgo.Session) (bool, error) {
	status, err := getCurrentStatus(session)
	if isConnectionNotAvailable(err) {
		// The connection dropped...
		logger.Errorf("DB connection dropped so reconnecting")
		session.Refresh()
		return false, nil
	}
	if err != nil {
		// Fail for any other reason.
		return false, errors.Trace(err)
	}

	majority := (len(status.Members) / 2) + 1
	healthy := 0
	// Check the members.
	for _, member := range status.Members {
		if member.Healthy {
			healthy += 1
		}
	}
	if healthy < majority {
		logger.Errorf("not enough members ready")
		return false, nil
	}
	return true, nil
}

var connectionErrors = []syscall.Errno{
	syscall.ECONNABORTED, // "software caused connection abort"
	syscall.ECONNREFUSED, // "connection refused"
	syscall.ECONNRESET,   // "connection reset by peer"
	syscall.ENETRESET,    // "network dropped connection on reset"
	syscall.ETIMEDOUT,    // "connection timed out"
}

func isConnectionNotAvailable(err error) bool {
	if err == nil {
		return false
	}
	// mgo returns io.EOF from session operations when the connection
	// has been dropped.
	if errors.Cause(err) == io.EOF {
		return true
	}
	// An errno may be returned so we check the connection-related ones.
	for _, errno := range connectionErrors {
		if errors.Cause(err) == errno {
			return true
		}
	}
	return false
}

// WaitUntilReady waits until all members of the replicaset are ready.
// It will retry every 10 seconds until the timeout is reached. Dropped
// connections will trigger a reconnect.
func WaitUntilReady(session *mgo.Session, timeout int) error {
	attempts := utils.AttemptStrategy{
		Delay: 10 * time.Second,
		Total: time.Duration(timeout) * time.Second,
	}
	var err error
	ready := false
	for a := attempts.Start(); !ready && a.Next(); {
		ready, err = isReady(session)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if !ready {
		return errors.Errorf("timed out after %d seconds", timeout)
	}
	return nil
}

// MemberState represents the state of a replica set member.
// See http://docs.mongodb.org/manual/reference/replica-states/
type MemberState int

const (
	StartupState = iota
	PrimaryState
	SecondaryState
	RecoveringState
	FatalState
	Startup2State
	UnknownState
	ArbiterState
	DownState
	RollbackState
	ShunnedState
)

var memberStateStrings = []string{
	StartupState:    "STARTUP",
	PrimaryState:    "PRIMARY",
	SecondaryState:  "SECONDARY",
	RecoveringState: "RECOVERING",
	FatalState:      "FATAL",
	Startup2State:   "STARTUP2",
	UnknownState:    "UNKNOWN",
	ArbiterState:    "ARBITER",
	DownState:       "DOWN",
	RollbackState:   "ROLLBACK",
	ShunnedState:    "SHUNNED",
}

// String returns a string describing the state.
func (state MemberState) String() string {
	if state < 0 || int(state) >= len(memberStateStrings) {
		return "INVALID_MEMBER_STATE"
	}
	return memberStateStrings[state]
}

// formatIPv6AddressWithoutBrackets turns correctly formatted IPv6 addresses
// into the "bad format" (without brackets around the address) that mongo <2.7
// require use.
func formatIPv6AddressWithoutBrackets(address string) string {
	address = strings.Replace(address, "[", "", 1)
	address = strings.Replace(address, "]", "", 1)
	return address
}

// formatIPv6AddressWithBrackets turns the "bad format" IPv6 addresses
// ("<addr>:<port>") that mongo <2.7 uses into correctly format addresses
// ("[<addr>]:<port>").
func formatIPv6AddressWithBrackets(address string) string {
	if strings.Count(address, ":") >= 2 && strings.Count(address, "[") == 0 {
		lastColon := strings.LastIndex(address, ":")
		host := address[:lastColon]
		port := address[lastColon+1:]
		return fmt.Sprintf("[%s]:%s", host, port)
	}
	return address
}
