// Copyright 2013-2015 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

// Package replicaset provides convenience functions and structures for
// creating and managing MongoDB replica sets via the mgo driver.
package replicaset

import (
	"fmt"
	"io"
	"net"
	"net/netip"
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
	getCurrentConfig  = CurrentConfig
	getSessionConfig  = sessionConfig
	getPrimarySession = primarySession
	getCurrentStatus  = CurrentStatus
	getBuildInfo      = BuildInfo
	isReady           = IsReady
	attemptInitiate   = doAttemptInitiate
)

// doAttemptInitiate will attempt to initiate a mongodb replicaset with each of
// the given configs, returning as soon as one config is successful.
func doAttemptInitiate(monotonicSession mgoSession, cfg []Config) error {
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
func Initiate(session mgoSession, address, name string, tags map[string]string) error {
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

func (m *Member) GoString() string {
	return fmtMembersForLog([]Member{*m})
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
		if hasVote(member) {
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
		if hasVote(member) {
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
	updatesApplied, err := doReplSetConfigChanges(cmd, session, currentConfig, updated, added, removed)
	if errors.Is(err, repairNeeded) {
		if updatesApplied {
			updated = nil
		}
		// A quorum-check failure can happen during update, add, or remove
		// when a member being removed is down. Repair once by forcing out
		// confirmed-dead removed members, then retry against the refreshed
		// config with a normal reconfig. If no member needs forcing out,
		// nothing is forced first.
		if repairErr := repairReplicaSet(session, removed); repairErr != nil {
			return errors.Annotatef(repairErr, "repairing replicaset after %v", err)
		}
		if len(updated) == 0 {
			repairedConfig, err := getSessionConfig(session)
			if err == nil {
				removedIds := set.NewInts(removed...)
				complete := true
				for _, member := range added {
					if !memberPresent(repairedConfig.Members, member) {
						complete = false
						break
					}
				}
				for _, member := range repairedConfig.Members {
					if removedIds.Contains(member.Id) {
						complete = false
						break
					}
				}
				if complete {
					return nil
				}
			}
		}
		retrySession, err := getPrimarySession(session)
		if err != nil {
			return err
		}
		defer retrySession.Close()
		repairedConfig, err := getSessionConfig(retrySession)
		if err != nil {
			return err
		}
		_, err = doReplSetConfigChanges(cmd, retrySession, repairedConfig, updated, added, removed)
		if errors.Is(err, repairNeeded) {
			// %v, not Annotatef: don't expose the repairNeeded sentinel to
			// callers.
			return errors.Errorf("cannot apply %s changes after replicaset repair attempt: %v", cmd, err)
		}
		return err
	}
	return err
}

var primarySessionAttempts = utils.AttemptStrategy{Total: time.Minute, Delay: time.Second}

func primarySession(session mgoSession) (mgoSession, error) {
	primary := session.Clone()
	primary.SetMode(mgo.Primary, true)
	if err := waitForPrimary(primary); err != nil {
		primary.Close()
		return nil, err
	}
	return primary, nil
}

func waitForPrimary(session mgoSession) error {
	var err error
	for attempt := primarySessionAttempts.Start(); attempt.Next(); {
		var result *IsMasterResults
		result, err = IsMaster(session)
		if err == nil {
			if result.IsMaster {
				return nil
			}
			err = errors.Errorf("server %q is not primary", result.Address)
		}
		// Refresh releases the socket, but mgo may still have stale primary
		// information until its next background topology check.
		session.Refresh()
	}
	return errors.Annotate(err, "cannot select primary after replicaset repair")
}

// doReplSetConfigChanges applies the updates, adds and removes one change at a
// time. Any step whose reconfig loses quorum returns repairNeeded so the caller
// can force-remove a dead member, if there is one, and retry.
// The boolean reports whether all updates were applied before returning.
func doReplSetConfigChanges(cmd string, session mgoSession, currentConfig *Config, updated []Member, added []Member, removed []int) (bool, error) {
	logger.Debugf("%s() changing replica set\n%s\n- updated:\n%#v\n- added:\n%s\n- removed: %v",
		cmd, fmtConfigForLog(currentConfig), fmtUpdatedMembersForLog(updated), fmtMembersForLog(added), removed)

	if currentConfig == nil {
		return false, errors.New("current config cannot be nil")
	}
	newConfig := *currentConfig
	// Mongo 4.4 onwards requires we process one change at a time.
	// First do the updates.
	for _, u := range updated {
		haveChanges := false
		for n := range newConfig.Members {
			// Replace a member with the same id.
			m := newConfig.Members[n]
			if m.Id != u.Id {
				continue
			}
			haveChanges = true
			newConfig.Members[n] = u
			break
		}
		if !haveChanges {
			return false, fmt.Errorf("cannot update member %#v in replicaset, no existing member", u)
		}
		newConfig.Version++
		err := applyReplSetConfig(cmd, session, newConfig)
		if err != nil {
			return false, fmt.Errorf("cannot update member %#v in replicaset: %w", u, err)
		}
	}
	// Then the adds.
	for _, m := range added {
		// A post-repair retry may find an already added member.
		if memberPresent(newConfig.Members, m) {
			continue
		}
		newConfig.Version++
		newConfig.Members = append(newConfig.Members, m)
		err := applyReplSetConfig(cmd, session, newConfig)
		if err != nil {
			return true, fmt.Errorf("cannot add member %#v to replicaset: %w", m, err)
		}
	}

	// Then do the removes.
	return true, applyRemoves(cmd, session, newConfig, removed)
}

func memberPresent(members []Member, member Member) bool {
	for _, existing := range members {
		if existing.Id == member.Id && sameAddress(existing.Address, member.Address) {
			return true
		}
	}
	return false
}

func applyRemoves(cmd string, session mgoSession, newConfig Config, removed []int) error {
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
			return fmt.Errorf("cannot remove member %d from replicaset: %w", id, err)
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
	BuildInfo() (mgo.BuildInfo, error)
}

var repairNeeded = errors.ConstError("repair needed")

// applyReplSetConfig applies the specified config to the mongo session.
// It checks if the replica set changes cause the DB connection to be dropped.
// If so, it Refreshes the session and tries to Ping again.
func applyReplSetConfig(cmd string, session mgoSession, newConfig Config) error {
	err := session.Run(bson.D{{"replSetReconfig", newConfig}}, nil)
	if isQuorumCheckFailure(err) {
		return fmt.Errorf("%w: %v", repairNeeded, err)
	}
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

func isQuorumCheckFailure(err error) bool {
	// Quorum was lost applying the reconfig. The server fails the quorum
	// check with code 74, which it also uses for other reconfig errors, so
	// that case is matched by its message. Code 11602 means a stepdown
	// interrupted the reconfig. Only a QueryError counts, so an unrelated
	// error can't trigger a force repair.
	qe, ok := err.(*mgo.QueryError)
	if !ok || qe == nil {
		return false
	}
	return qe.Code == 11602 || strings.Contains(qe.Message, "Quorum check failed")
}

// repairReplicaSet removes explicit targets that remain unhealthy and report
// DOWN or UNKNOWN with a forced reconfig, then waits for a primary to be
// elected. When no target stays unavailable and nothing else blocks the
// repair, it forces nothing and only waits for a primary, so the caller
// retries the change with a normal, quorum-checked reconfig.
func repairReplicaSet(session mgoSession, removed []int) error {
	if len(removed) == 0 {
		// Quorum was lost without us asking to remove anything, so there is no
		// member we are allowed to force out.
		return errors.Errorf("cannot repair replicaset: no removal targets")
	}

	cfg, err := getSessionConfig(session)
	if err != nil {
		return errors.Annotatef(err, "getting rs config to repair")
	}

	removedIds := set.NewInts(removed...)

	// A forced reconfig skips the quorum check mongo would normally run, so
	// decide first whether forcing is safe at all (see assessRepairSafety).
	assessment, err := assessRepairSafety(session, cfg, removedIds)
	if err != nil {
		return err
	}
	if !assessment.safe {
		return errors.Errorf("cannot repair replicaset: %s", assessment.reason)
	}
	if assessment.evict.Size() == 0 {
		// No target stayed DOWN or UNKNOWN, so the quorum failure may be
		// transient. Forcing would skip the mongo quorum check, so leave the
		// retry to a normal reconfig instead. The primary may have stepped
		// down, so still wait for one before the caller retries.
		logger.Infof("no member of %v in the current config remained DOWN or UNKNOWN, waiting for a primary before retrying without a forced reconfig",
			removedIds.SortedValues())
	} else {
		cfg.Version++
		var remaining []Member
		for _, m := range cfg.Members {
			if assessment.evict.Contains(m.Id) {
				continue
			}
			remaining = append(remaining, m)
		}
		cfg.Members = remaining
		err = session.Run(bson.D{
			{"replSetReconfig", *cfg},
			{"force", true},
		}, nil)
		if err == io.EOF {
			// The connection can drop during the forced reconfig.
			logger.Debugf("got EOF while repairing rs, calling session.Refresh()")
			session.Refresh()
		} else if err != nil {
			return errors.Annotatef(err, "repairing rs")
		}
		logger.Infof("replicaset repair done, waiting for new primary")
	}
	backoff := primaryPollInterval
	var statusErr error
	for i := 0; i < primaryPollAttempts; i++ {
		var status *Status
		status, statusErr = getCurrentStatus(session)
		if statusErr != nil {
			logger.Warningf("checking rs status to repair: %v", statusErr)
			session.Refresh()
		} else {
			for _, m := range status.Members {
				if m.State == PrimaryState {
					return nil
				}
			}
			logger.Debugf("still waiting for primary...")
		}
		if i+1 < primaryPollAttempts {
			time.Sleep(backoff)
			backoff = time.Duration(float64(backoff) * 1.2)
		}
	}
	if statusErr != nil {
		return errors.Annotatef(statusErr, "checking rs status to repair")
	}
	return errors.Errorf("cannot repair replicaset: no primary elected after %d attempts", primaryPollAttempts)
}

const (
	// confirmDeadSamples is the most status samples assessRepairSafety takes.
	confirmDeadSamples = 3

	// About 16.5s in total (1s growing 1.2x over 8 sleeps), longer than
	// mongo's default 10s election timeout.
	primaryPollAttempts = 9
)

// These are variables, not constants, so tests can patch them to zero and
// avoid sleeping.
var (
	confirmDeadInterval = 2 * time.Second
	primaryPollInterval = time.Second
)

// repairSafetyAssessment says whether the repair may proceed, and which
// members it may force out. When it may not, reason explains why. With
// nothing to evict, proceeding means only a normal retry, not a forced
// reconfig.
type repairSafetyAssessment struct {
	safe   bool
	reason string
	evict  set.Ints
}

// assessRepairSafety re-samples the replicaset status to decide whether a
// forced reconfig is needed and safe.
// Only members in cfg are considered. A targeted member is evictable only when
// it is unhealthy and reports DOWN or UNKNOWN in every sample. It does not
// count as healthy for majority.
// Repair is refused if an untargeted voter is unhealthy in every sample. If
// no target qualifies, the repair is then safe with nothing to evict.
// Otherwise it is also refused if the consistently healthy remaining voters
// lack a majority, or none of those voters can become primary.
func assessRepairSafety(session mgoSession, cfg *Config, removed set.Ints) (*repairSafetyAssessment, error) {
	// Voting members form the election majority, while only data-bearing voters
	// with positive priority can become primary.
	members := set.NewInts()
	voting := set.NewInts()
	primaryEligible := set.NewInts()
	for _, m := range cfg.Members {
		members.Add(m.Id)
		if hasVote(m) {
			voting.Add(m.Id)
		}
		if canBecomePrimary(m) {
			primaryEligible.Add(m.Id)
		}
	}

	var deadRemoved, liveVoting, livePrimaryEligible, unhealthyOtherVoting set.Ints
	for i := 0; i < confirmDeadSamples; i++ {
		if i > 0 {
			time.Sleep(confirmDeadInterval)
		}
		status, err := getCurrentStatus(session)
		if err != nil {
			return nil, errors.Annotatef(err, "getting rs status to assess repair safety")
		}

		deadRemovedNow := set.NewInts()
		liveVotingNow := set.NewInts()
		livePrimaryEligibleNow := set.NewInts()
		unhealthyOtherVotingNow := set.NewInts()
		for _, m := range status.Members {
			// The status can list a member that the config we are about to
			// force no longer has, so ignore it rather than act on it.
			if !members.Contains(m.Id) {
				continue
			}
			if m.Healthy {
				if voting.Contains(m.Id) {
					liveVotingNow.Add(m.Id)
				}
				if primaryEligible.Contains(m.Id) && (m.State == PrimaryState || m.State == SecondaryState) {
					livePrimaryEligibleNow.Add(m.Id)
				}
				continue
			}
			if removed.Contains(m.Id) && (m.State == DownState || m.State == UnknownState) {
				deadRemovedNow.Add(m.Id)
			}
			if !voting.Contains(m.Id) {
				continue
			}
			if !removed.Contains(m.Id) {
				unhealthyOtherVotingNow.Add(m.Id)
			}
		}

		if i == 0 {
			deadRemoved, liveVoting, livePrimaryEligible, unhealthyOtherVoting =
				deadRemovedNow, liveVotingNow, livePrimaryEligibleNow, unhealthyOtherVotingNow
		} else {
			deadRemoved = deadRemoved.Intersection(deadRemovedNow)
			liveVoting = liveVoting.Intersection(liveVotingNow)
			livePrimaryEligible = livePrimaryEligible.Intersection(livePrimaryEligibleNow)
			unhealthyOtherVoting = unhealthyOtherVoting.Intersection(unhealthyOtherVotingNow)
		}
		if deadRemoved.Size() == 0 && unhealthyOtherVoting.Size() == 0 {
			// Nothing left to evict or to block on, so more samples cannot
			// change the outcome.
			break
		}
	}

	// Check the untargeted voters first, so a persistently down voter is
	// reported even when every target has recovered.
	if unhealthyOtherVoting.Size() > 0 {
		// Members we were not asked to remove are also unhealthy, so we cannot
		// tell this apart from a wider outage.
		return &repairSafetyAssessment{
			reason: fmt.Sprintf("voters %v are unhealthy but not in the removal targets %v",
				unhealthyOtherVoting.SortedValues(), removed.SortedValues()),
		}, nil
	}
	if deadRemoved.Size() == 0 {
		return &repairSafetyAssessment{safe: true, evict: deadRemoved}, nil
	}

	// The eviction must leave enough live voters to elect a primary.
	evictVoting := deadRemoved.Intersection(voting)
	remainingVoters := voting.Difference(evictVoting)
	liveVoters := liveVoting.Difference(evictVoting)
	needed := (remainingVoters.Size() / 2) + 1
	if liveVoters.Size() < needed {
		return &repairSafetyAssessment{
			reason: fmt.Sprintf("evicting %v leaves live voters %v of %v, but a majority needs %d",
				evictVoting.SortedValues(), liveVoters.SortedValues(), remainingVoters.SortedValues(), needed),
		}, nil
	}
	if livePrimaryEligible.Size() == 0 {
		return &repairSafetyAssessment{
			reason: fmt.Sprintf("evicting %v leaves no live primary-eligible member among voters %v",
				deadRemoved.SortedValues(), remainingVoters.SortedValues()),
		}, nil
	}

	return &repairSafetyAssessment{safe: true, evict: deadRemoved}, nil
}

func sameAddress(a, b string) bool {
	if a == b {
		return true
	}
	ha, pa, err := splitAddress(a)
	if err != nil {
		return false
	}
	hb, pb, err := splitAddress(b)
	if err != nil {
		return false
	}
	if pa != pb {
		return false
	}
	ipA, errA := netip.ParseAddr(ha)
	ipB, errB := netip.ParseAddr(hb)
	switch {
	case errA == nil && errB == nil:
		// Distinct loopback IPs can host different servers on the same port.
		return ipA.Unmap() == ipB.Unmap()
	case errA == nil:
		return strings.EqualFold(hb, "localhost") && localhostIP(ipA)
	case errB == nil:
		return strings.EqualFold(ha, "localhost") && localhostIP(ipB)
	}
	return ha == hb
}

func localhostIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	return ip == netip.AddrFrom4([4]byte{127, 0, 0, 1}) || ip == netip.AddrFrom16([16]byte{15: 1})
}

// splitAddress splits addr into host and port. MongoDB reads an address
// without a port as port 27017. SplitHostPort rejects a bare IPv6 address
// even with a port added, so a bare IP address is taken as the host.
func splitAddress(addr string) (string, string, error) {
	if host, port, err := net.SplitHostPort(addr); err == nil {
		return host, port, nil
	}
	if _, err := netip.ParseAddr(addr); err == nil {
		return addr, "27017", nil
	}
	return net.SplitHostPort(addr + ":27017")
}

// Add adds the given members to the session's replica set.  Duplicates of
// existing replicas will be ignored.
//
// Members will have their Ids set automatically if they are not already > 0.
// Add removes no members, so if a reconfig loses quorum it returns an error
// rather than force-removing anything.
func Add(session mgoSession, members ...Member) error {
	config, err := getCurrentConfig(session)
	if err != nil {
		return err
	}

	max := findMaxId(config.Members, members)

	var added []Member
outerLoop:
	for _, newMember := range members {
		for _, member := range config.Members {
			if sameAddress(member.Address, newMember.Address) {
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
// If a reconfig loses quorum, members being removed that stay DOWN or UNKNOWN
// may be force-removed and, unless the repair is refused or fails, the change
// is retried once.
// Retrying pending changes on another primary requires a session that allows
// replica set discovery rather than a direct connection.
func Remove(session mgoSession, addrs ...string) error {
	config, err := getCurrentConfig(session)
	if err != nil {
		return err
	}
	var toRemove []int
	for _, rem := range addrs {
		for _, repl := range config.Members {
			if sameAddress(repl.Address, rem) {
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

func hasVote(m Member) bool {
	return m.Votes == nil || *m.Votes > 0
}

func canBecomePrimary(m Member) bool {
	if !hasVote(m) || (m.Arbiter != nil && *m.Arbiter) {
		return false
	}
	return m.Priority == nil || *m.Priority > 0
}

// Set changes the current set of replica set members.  Members will have their
// ids set automatically if their ids are not already > 0.
// If a reconfig loses quorum, members being removed that stay DOWN or UNKNOWN
// may be force-removed and, unless the repair is refused or fails, the change
// is retried once.
// Retrying pending changes on another primary requires a session that allows
// replica set discovery rather than a direct connection.
func Set(session mgoSession, members []Member) error {
	config, err := getCurrentConfig(session)
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
	// An existing member that moves to another address frees its old one.
	for _, m := range members {
		if existing, ok := existingMembers[m.Id]; ok && m.Id > 0 && existing.Address != m.Address {
			delete(existingAddressIds, existing.Address)
		}
	}
	for _, m := range members {
		// A wanted member at an existing address updates that member in
		// place, reusing its Id, rather than adding a duplicate host.
		id, ok := existingAddressIds[m.Address]
		// Otherwise take the first existing address that sameAddress matches.
		if !ok {
			for _, e := range config.Members {
				if _, left := existingAddressIds[e.Address]; left && sameAddress(e.Address, m.Address) {
					id, ok = e.Id, true
					break
				}
			}
		}
		if ok && m.Id != id {
			if m.Id > 0 {
				return fmt.Errorf("cannot set member %d: address %q already used by member %d", m.Id, m.Address, id)
			}
			m.Id = id
		}
		// A member given without an Id has Id 0, so it is member 0 only
		// when its address matched member 0.
		existingMember, exists := existingMembers[m.Id]
		if exists && (ok || m.Id > 0) {
			if existingMember.Address != m.Address || hasVote(existingMember) != hasVote(m) {
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
func IsMaster(session mgoSession) (*IsMasterResults, error) {
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
func MasterHostPort(session mgoSession) (string, error) {
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
func CurrentMembers(session mgoSession) ([]Member, error) {
	cfg, err := getCurrentConfig(session)
	if err != nil {
		return nil, err
	}
	return cfg.Members, nil
}

// CurrentConfig returns the Config for the given session's replica set.  If
// there is no current config, the error returned will be mgo.ErrNotFound.
func CurrentConfig(session mgoSession) (*Config, error) {
	monotonicSession := session.Clone()
	defer monotonicSession.Close()
	monotonicSession.SetMode(mgo.Monotonic, true)
	return sessionConfig(monotonicSession)
}

// sessionConfig reads the config through session itself, so from the server
// its commands go to. Repair runs its reconfig commands there, so it must not
// read an older copy from another member. Mongo serves local reads on a
// secondary, so this still works after that server steps down on losing
// quorum.
func sessionConfig(session mgoSession) (*Config, error) {
	cfg := &Config{}
	err := session.DB("local").C("system.replset").Find(nil).One(cfg)
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
func StepDownPrimary(session mgoSession) error {
	strictSession := session.Clone()
	defer strictSession.Close()
	// StepDown can only be called on the primary
	strictSession.SetMode(mgo.Primary, true)
	// replSetStepDown takes a few optional parameters that vary based on what
	// version of Mongo is running. In Mongo 2.4 it just takes the "step down
	// seconds" which claims to default to 60s.
	// In 3.2 it can also take secondaryCatchUpPeriodSecs which is supposed to
	// start at 10s. However, testing shows that not passing either gives:
	// err{"stepdown period must be longer than secondaryCatchUpPeriodSecs"}
	err := strictSession.Run(bson.D{{"replSetStepDown", 60.0}}, nil)
	// we expect to get io.EOF so don't treat it as a failure.
	if err == io.EOF {
		return nil
	}
	return err
}

// BuildInfo returns the mongod build info for the given session.
func BuildInfo(session mgoSession) (mgo.BuildInfo, error) {
	return session.BuildInfo()
}

// CurrentStatus returns the status of the replica set for the given session.
func CurrentStatus(session mgoSession) (*Status, error) {
	status := &Status{}
	err := session.Run("replSetGetStatus", status)
	if err != nil {
		return nil, errors.Annotate(err, "cannot get replica set status")
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
func IsReady(session mgoSession) (bool, error) {
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
func WaitUntilReady(session mgoSession, timeout int) error {
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
