// Copyright 2013-2015 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package replicaset

import (
	"fmt"
	"io"
	"sort"
	"strings"
	stdtesting "testing"
	"time"

	"github.com/juju/errors"
	"github.com/juju/mgo/v3"
	"github.com/juju/mgo/v3/bson"
	mgotesting "github.com/juju/mgo/v3/testing"
	"github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	"github.com/juju/utils/v3"
	"github.com/juju/utils/v3/cert"
	gc "gopkg.in/check.v1"
)

const rsName = "juju"

func TestPackage(t *stdtesting.T) {
	gc.TestingT(t)
}

type MongoSuite struct {
	testing.IsolationSuite
	root *mgotesting.MgoInstance
}

func newServer(c *gc.C, params ...string) *mgotesting.MgoInstance {
	inst := &mgotesting.MgoInstance{Params: append([]string{"--replSet", rsName}, params...)}
	err := inst.Start(nil)
	c.Assert(err, jc.ErrorIsNil)

	session, err := inst.DialDirect()
	if err != nil {
		inst.Destroy()
		c.Fatalf("error dialing mongo server: %v", err.Error())
		return nil
	}
	defer session.Close()

	session.SetMode(mgo.Monotonic, true)
	if err = session.Ping(); err != nil {
		inst.Destroy()
		c.Fatalf("error pinging mongo server: %v", err.Error())
	}
	buildInfo, err := session.BuildInfo()
	c.Assert(err, jc.ErrorIsNil)
	if !buildInfo.VersionAtLeast(4) {
		c.Fatalf("only mongo version 4 or greater supported, found %v", buildInfo.Version)
	}
	return inst
}

var _ = gc.Suite(&MongoSuite{})

func (s *MongoSuite) SetUpTest(c *gc.C) {
	s.IsolationSuite.SetUpTest(c)
	s.PatchEnvironment("JUJU_MONGO_STORAGE_ENGINE", "wiredTiger")
	s.root = newServer(c)
	s.AddCleanup(func(c *gc.C) { s.root.Destroy() })
	dialAndTestInitiate(c, s.root, s.root.Addr())
}

var initialTags = map[string]string{"foo": "bar"}

// assertMembers asserts the known field values of a retrieved and expected
// Members slice are equal.
func assertMembers(c *gc.C, mems []Member, expectedMembers []Member) {
	// 2.x and 3.2 seem to use different default values for bool. For
	// example, in 3.2 false is false, not nil, and we can't know the
	// pointer value to check with DeepEquals.
	c.Logf("comparing: %s\nto: %s",
		fmtConfigForLog(&Config{Name: "obtained", Members: mems[:]}),
		fmtConfigForLog(&Config{Name: "expected", Members: expectedMembers[:]}))
	for i := range mems {
		c.Check(mems[i].Id, gc.Equals, expectedMembers[i].Id)
		c.Check(mems[i].Address, gc.Equals, expectedMembers[i].Address)
		c.Check(mems[i].Tags, jc.DeepEquals, expectedMembers[i].Tags)
	}
}

func dialAndTestInitiate(c *gc.C, inst *mgotesting.MgoInstance, addr string) {
	session := inst.MustDialDirect()
	defer session.Close()

	mode := session.Mode()
	err := Initiate(session, addr, rsName, initialTags)
	c.Assert(err, jc.ErrorIsNil)

	// make sure we haven't messed with the session's mode
	c.Assert(session.Mode(), gc.Equals, mode)

	// Ids start at 1 for us, so we can differentiate between set and unset
	expectedMembers := []Member{{Id: 1, Address: addr, Tags: initialTags}}

	// need to set mode to strong so that we wait for the write to succeed
	// before reading and thus ensure that we're getting consistent reads.
	session.SetMode(mgo.Strong, false)

	mems, err := CurrentMembers(session)
	c.Assert(err, jc.ErrorIsNil)
	assertMembers(c, mems, expectedMembers)

	loadData(session, c)
}

func (s *MongoSuite) TestInitiateSetsProtocolVersion(c *gc.C) {
	s.root.Destroy()

	// create a new server that hasn't been initiated
	s.root = newServer(c)
	session := s.root.MustDialDirect()
	defer session.Close()

	mockBuildInfo := func(session mgoSession) (mgo.BuildInfo, error) {
		return mgo.BuildInfo{
			Version:      "4",
			GitVersion:   "4.0.0",
			VersionArray: []int{4, 0, 0},
		}, nil
	}
	called := false
	mockAttemptInitiate := func(monotonicSession mgoSession, cfg []Config) error {
		c.Assert(cfg, gc.HasLen, 2)
		if cfg[0].ProtocolVersion != 1 {
			c.Fatalf("obtained protocol version %d, expected 1", cfg[0].ProtocolVersion)
		}
		called = true
		return nil
	}
	mockCurrentStatus := func(session mgoSession) (*Status, error) {
		return &Status{
			Name:    "test",
			Members: []MemberStatus{{}},
		}, nil
	}

	s.PatchValue(&getBuildInfo, mockBuildInfo)
	s.PatchValue(&attemptInitiate, mockAttemptInitiate)
	s.PatchValue(&getCurrentStatus, mockCurrentStatus)
	err := Initiate(session, s.root.Addr(), rsName, initialTags)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(called, jc.IsTrue)
}

func (s *MongoSuite) TestInitiateWaitsForStatus(c *gc.C) {
	s.root.Destroy()

	// create a new server that hasn't been initiated
	s.root = newServer(c)
	session := s.root.MustDialDirect()
	defer session.Close()

	i := 0
	mockStatus := func(session mgoSession) (*Status, error) {
		status := &Status{}
		var err error
		i += 1
		if i < 20 {
			err = fmt.Errorf("bang!")
		} else if i > 20 {
			// when i == 20 then len(status.Members) == 0
			// so we will be called one more time until we populate
			// Members
			status.Members = append(status.Members, MemberStatus{Id: 1})
		}
		return status, err
	}

	s.PatchValue(&getCurrentStatus, mockStatus)
	err := Initiate(session, s.root.Addr(), rsName, initialTags)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(i, gc.Equals, 21)
}

func loadData(session mgoSession, c *gc.C) {
	type foo struct {
		Name    string
		Address string
		Count   int
	}

	for col := 0; col < 10; col++ {
		// Testing with mongodb3.2 showed the need to make foos a slice
		// if interface{} (Insert expects a slice not an empty
		// interface) passed in expanded. Passing a slice of foo to
		// Insert gives this error, with the slice perhaps not handled
		// by writeOp().
		// `Message:"error parsing element 0 of field documents :: caused by :: wrong type for '0' field, expected object`
		foos := make([]interface{}, 10000)
		for n := range foos {
			foos[n] = foo{
				Name:    fmt.Sprintf("name_%d_%d", col, n),
				Address: fmt.Sprintf("address_%d_%d", col, n),
				Count:   n * (col + 1),
			}
		}

		err := session.DB("testing").C(fmt.Sprintf("data%d", col)).Insert(foos...)
		c.Assert(err, jc.ErrorIsNil)
	}
}

func attemptLoop(c *gc.C, strategy utils.AttemptStrategy, desc string, f func() error) {
	var err error
	start := time.Now()
	attemptCount := 0
	for attempt := strategy.Start(); attempt.Next(); {
		attemptCount += 1
		if err = f(); err == nil || !attempt.HasNext() {
			break
		}
		c.Logf("%s failed: %v", desc, err)
	}
	c.Logf("%s: %d attempts in %s", desc, attemptCount, time.Since(start))
	c.Assert(err, jc.ErrorIsNil)
}

func (s *MongoSuite) TestAddRemoveSet(c *gc.C) {
	getAddr := func(inst *mgotesting.MgoInstance) string {
		return inst.Addr()
	}
	assertAddRemoveSet(c, s.root, getAddr)
}

func assertAddRemoveSet(c *gc.C, root *mgotesting.MgoInstance, getAddr func(*mgotesting.MgoInstance) string, params ...string) {
	session := root.MustDial()
	defer session.Close()

	members := make([]Member, 0, 5)

	// Add should be idempotent, so re-adding root here shouldn't result in
	// two copies of root in the replica set
	members = append(members, Member{Address: getAddr(root), Tags: initialTags})

	// We allow for up to 2 minutes  per operation, since Add, Set, etc. call
	// replSetReconfig which may cause primary renegotiation. According
	// to the Mongo docs, "typically this is 10-20 seconds, but could be
	// as long as a minute or more."
	//
	// Note that the delay is set at 500ms to cater for relatively quick
	// operations without thrashing on those that take longer.
	strategy := utils.AttemptStrategy{Total: time.Minute * 2, Delay: time.Millisecond * 500}

	instances := make([]*mgotesting.MgoInstance, 5)
	instances[0] = root
	for i := 1; i < len(instances); i++ {
		inst := newServer(c, params...)
		instances[i] = inst
		// no need to Remove the instances from the replicaset as
		// we're destroying the replica set immediately afterwards
		defer inst.Destroy()
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		tags := map[string]string{key: val}
		members = append(members, Member{Address: getAddr(inst), Tags: tags})
	}

	attemptLoop(c, strategy, "Add()", func() error {
		return Add(session, members...)
	})

	expectedMembers := make([]Member, len(members))
	for i, m := range members {
		// Ids should start at 1 (for the root) and go up
		m.Id = i + 1
		expectedMembers[i] = m
	}

	var cfg *Config
	attemptLoop(c, strategy, "CurrentConfig()", func() error {
		var err error
		cfg, err = CurrentConfig(session)
		return err
	})
	c.Assert(cfg.Name, gc.Equals, rsName)
	// 5 since it increments for each added/removed member.
	c.Assert(cfg.Version, gc.Equals, 5)

	mems := cfg.Members
	assertMembers(c, mems, expectedMembers)

	// Now remove the last two Members...
	attemptLoop(c, strategy, "Remove()", func() error {
		return Remove(session, members[3].Address, members[4].Address)
	})
	expectedMembers = expectedMembers[0:3]

	// ... and confirm that CurrentMembers reflects the removal.
	attemptLoop(c, strategy, "CurrentMembers()", func() error {
		var err error
		mems, err = CurrentMembers(session)
		return err
	})
	assertMembers(c, mems, expectedMembers)

	// now let's mix it up and set the new members to a mix of the previous
	// plus the new arbiter
	// Also have an explicitly large member Id to make sure we don't have possible
	// collisions
	mem4 := members[4]
	mem4.Id = 10
	mems = []Member{members[3], mems[2], mems[0], mem4}
	attemptLoop(c, strategy, "Set()", func() error {
		err := Set(session, mems)
		if err != nil {
			c.Logf("current session mode: %v", session.Mode())
			session.Refresh()
		}
		return err
	})

	attemptLoop(c, strategy, "Ping()", func() error {
		// can dial whichever replica address here, mongo will figure it out
		if session != nil {
			session.Close()
		}
		session = instances[0].MustDialDirect()
		return session.Ping()
	})

	// any new members will get an id of max(other_ids...)+1
	expectedMembers = []Member{members[3], expectedMembers[2], expectedMembers[0], members[4]}
	expectedMembers[0].Id = 11
	expectedMembers[3].Id = 10
	// CurrentMembers always sorts the member ids by Id so they can be nicely displayed and tracked.
	sort.Slice(expectedMembers, func(i, j int) bool { return expectedMembers[i].Id < expectedMembers[j].Id })

	attemptLoop(c, strategy, "CurrentMembers()", func() error {
		var err error
		mems, err = CurrentMembers(session)
		return err
	})
	assertMembers(c, mems, expectedMembers)
}

func (s *MongoSuite) TestIsMaster(c *gc.C) {
	session := s.root.MustDial()
	defer session.Close()

	expected := IsMasterResults{
		// The following fields hold information about the specific mongodb node.
		IsMaster:  true,
		Secondary: false,
		Arbiter:   false,
		Address:   s.root.Addr(),
		LocalTime: time.Time{},

		// The following fields hold information about the replica set.
		ReplicaSetName: rsName,
		Addresses:      []string{s.root.Addr()},
		Arbiters:       nil,
		PrimaryAddress: s.root.Addr(),
	}

	res, err := IsMaster(session)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(closeEnough(res.LocalTime, time.Now()), jc.IsTrue)
	res.LocalTime = time.Time{}
	c.Check(*res, jc.DeepEquals, expected)
}

func (s *MongoSuite) TestMasterHostPort(c *gc.C) {
	session := s.root.MustDial()
	defer session.Close()

	expected := s.root.Addr()
	result, err := MasterHostPort(session)

	c.Logf("TestMasterHostPort expected: %v, got: %v", expected, result)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(result, gc.Equals, expected)
}

func (s *MongoSuite) TestMasterHostPortOnUnconfiguredReplicaSet(c *gc.C) {
	inst := &mgotesting.MgoInstance{}
	err := inst.Start(nil)
	c.Assert(err, jc.ErrorIsNil)
	defer inst.Destroy()
	session := inst.MustDial()
	hp, err := MasterHostPort(session)
	c.Assert(err, gc.Equals, ErrMasterNotConfigured)
	c.Assert(hp, gc.Equals, "")
}

func (s *MongoSuite) TestIsReadyOne(c *gc.C) {
	s.PatchValue(&getCurrentStatus,
		func(session mgoSession) (*Status, error) {
			status := &Status{Members: []MemberStatus{{
				Id:      1,
				Healthy: true,
			}}}
			return status, nil
		},
	)
	session := s.root.MustDial()
	defer session.Close()

	ready, err := IsReady(session)
	c.Assert(err, jc.ErrorIsNil)

	c.Check(ready, jc.IsTrue)
}

func (s *MongoSuite) TestIsReadyMultiple(c *gc.C) {
	s.PatchValue(&getCurrentStatus,
		func(session mgoSession) (*Status, error) {
			status := &Status{}
			for i := 1; i < 5; i++ {
				member := MemberStatus{Id: i + 1, Healthy: true}
				status.Members = append(status.Members, member)
			}
			return status, nil
		},
	)
	session := s.root.MustDial()
	defer session.Close()

	ready, err := IsReady(session)
	c.Assert(err, jc.ErrorIsNil)

	c.Check(ready, jc.IsTrue)
}

func (s *MongoSuite) TestIsReadyNotOne(c *gc.C) {
	s.PatchValue(&getCurrentStatus,
		func(session mgoSession) (*Status, error) {
			status := &Status{Members: []MemberStatus{{
				Id:      1,
				Healthy: false,
			}}}
			return status, nil
		},
	)
	session := s.root.MustDial()
	defer session.Close()

	ready, err := IsReady(session)
	c.Assert(err, jc.ErrorIsNil)

	c.Check(ready, jc.IsFalse)
}

func (s *MongoSuite) TestIsReadyMinority(c *gc.C) {
	s.PatchValue(&getCurrentStatus,
		func(session mgoSession) (*Status, error) {
			status := &Status{Members: []MemberStatus{{
				Id:      1,
				Healthy: true,
			},
				{
					Id:      2,
					Healthy: false,
				},
				{
					Id:      3,
					Healthy: false,
				}}}
			return status, nil
		},
	)
	session := s.root.MustDial()
	defer session.Close()

	ready, err := IsReady(session)
	c.Assert(err, jc.ErrorIsNil)

	c.Check(ready, jc.IsFalse)
}

type statusErrorSession struct {
	mgoSession
	err       error
	refreshes int
}

func (s *statusErrorSession) Run(cmd interface{}, result interface{}) error {
	return s.err
}

func (s *statusErrorSession) Refresh() {
	s.refreshes++
	s.mgoSession.Refresh()
}

func (s *MongoSuite) checkConnectionFailure(c *gc.C, failure error) {
	session := s.root.MustDial()
	defer session.Close()

	failedSession := &statusErrorSession{mgoSession: session, err: failure}
	ready, err := IsReady(failedSession)
	c.Assert(err, jc.ErrorIsNil)

	c.Check(ready, jc.IsFalse)
	c.Check(failedSession.refreshes, gc.Equals, 1)
}

func (s *MongoSuite) TestIsReadyConnectionDropped(c *gc.C) {
	s.checkConnectionFailure(c, io.EOF)
}

func (s *MongoSuite) TestIsReadyConnectionFailedWithErrno(c *gc.C) {
	for _, errno := range connectionErrors {
		c.Logf("Checking errno %#v (%v)", errno, errno)
		s.checkConnectionFailure(c, errno)
	}
}

func (s *MongoSuite) TestIsReadyError(c *gc.C) {
	failure := errors.New("failed!")
	session := s.root.MustDial()
	defer session.Close()

	failedSession := &statusErrorSession{mgoSession: session, err: failure}
	_, err := IsReady(failedSession)
	c.Check(errors.Cause(err), gc.Equals, failure)
	c.Check(failedSession.refreshes, gc.Equals, 0)
}

func (s *MongoSuite) TestWaitUntilReady(c *gc.C) {
	var isReadyCalled bool
	mockIsReady := func(session mgoSession) (bool, error) {
		isReadyCalled = true
		return true, nil
	}

	s.PatchValue(&isReady, mockIsReady)
	session := s.root.MustDial()
	defer session.Close()

	err := WaitUntilReady(session, 10)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(isReadyCalled, jc.IsTrue)
}

func (s *MongoSuite) TestWaitUntilReadyTimeout(c *gc.C) {
	mockIsReady := func(session mgoSession) (bool, error) {
		return false, nil
	}

	s.PatchValue(&isReady, mockIsReady)
	session := s.root.MustDial()
	defer session.Close()

	err := WaitUntilReady(session, 0)
	c.Assert(err, gc.ErrorMatches, "timed out after 0 seconds")
}

func (s *MongoSuite) TestWaitUntilReadyError(c *gc.C) {
	mockIsReady := func(session mgoSession) (bool, error) {
		return false, errors.New("foobar")
	}

	s.PatchValue(&isReady, mockIsReady)
	session := s.root.MustDial()
	defer session.Close()

	err := WaitUntilReady(session, 0)
	c.Assert(err, gc.ErrorMatches, "foobar")
}

func (s *MongoSuite) TestCurrentStatus(c *gc.C) {
	session := s.root.MustDial()
	defer session.Close()

	inst1 := newServer(c)
	defer inst1.Destroy()
	defer func() { _ = Remove(session, inst1.Addr()) }()

	inst2 := newServer(c)
	defer inst2.Destroy()
	defer func() { _ = Remove(session, inst2.Addr()) }()

	var err error
	strategy := utils.AttemptStrategy{Total: time.Minute * 2, Delay: time.Millisecond * 500}
	attempt := strategy.Start()
	for attempt.Next() {
		err = Add(session, Member{Address: inst1.Addr()}, Member{Address: inst2.Addr()})
		if err == nil || !attempt.HasNext() {
			break
		}
	}
	c.Assert(err, jc.ErrorIsNil)

	expected := &Status{
		Name: rsName,
		Members: []MemberStatus{{
			Id:      1,
			Address: s.root.Addr(),
			Self:    true,
			ErrMsg:  "",
			Healthy: true,
			State:   PrimaryState,
		}, {
			Id:      2,
			Address: inst1.Addr(),
			Self:    false,
			ErrMsg:  "",
			Healthy: true,
			State:   SecondaryState,
		}, {
			Id:      3,
			Address: inst2.Addr(),
			Self:    false,
			ErrMsg:  "",
			Healthy: true,
			State:   SecondaryState,
		}},
	}

	strategy.Total = time.Second * 90
	attempt = strategy.Start()
	var res *Status
	for attempt.Next() {
		var err error
		res, err = CurrentStatus(session)
		if err != nil {
			if !attempt.HasNext() {
				c.Errorf("Couldn't get status before timeout, got err: %v", err)
				return
			} else {
				// try again
				continue
			}
		}

		if res.Members[0].State == PrimaryState &&
			res.Members[1].State == SecondaryState &&
			res.Members[2].State == SecondaryState {
			break
		}
		if !attempt.HasNext() {
			c.Errorf("Servers did not get into final state before timeout.  Status: %#v", res)
			return
		}
	}

	for x := range res.Members {
		// non-empty uptime and ping
		c.Check(res.Members[x].Uptime, gc.Not(gc.Equals), 0)

		// ping is always going to be zero since we're on localhost
		// so we can't really test it right now

		// now overwrite Uptime so it won't throw off DeepEquals
		res.Members[x].Uptime = 0
	}
	c.Check(res, jc.DeepEquals, expected)
}

func closeEnough(expected, obtained time.Time) bool {
	t := obtained.Sub(expected)
	return (-500*time.Millisecond) < t && t < (500*time.Millisecond)
}

func findPrimary(c *gc.C, session mgoSession) int {
	status, err := CurrentStatus(session)
	c.Assert(err, jc.ErrorIsNil)
	for i, m := range status.Members {
		if m.State == PrimaryState {
			return i
		}
	}
	return -1
}

func (s *MongoSuite) TestStepDownPrimary(c *gc.C) {
	session := s.root.MustDial()
	defer func() {
		if session != nil {
			session.Close()
			session = nil
		}
	}()
	s0 := s.root
	s1 := newServer(c)
	defer s1.Destroy()
	s2 := newServer(c)
	defer s2.Destroy()
	strategy := utils.AttemptStrategy{Total: time.Minute * 2, Delay: time.Millisecond * 500}
	attemptLoop(c, strategy, "Add()", func() error {
		return Add(session, Member{
			Address: s1.Addr(),
			Tags:    map[string]string{"s1": "s1"},
		}, Member{
			Address: s2.Addr(),
			Tags:    map[string]string{"s2": "s2"},
		})
	})
	mems, err := CurrentMembers(session)
	c.Assert(err, jc.ErrorIsNil)
	assertMembers(c, mems, []Member{{
		Id:      1,
		Address: s0.Addr(),
		Tags:    initialTags,
	}, {
		Id:      2,
		Address: s1.Addr(),
		Tags:    map[string]string{"s1": "s1"},
	}, {
		Id:      3,
		Address: s2.Addr(),
		Tags:    map[string]string{"s2": "s2"},
	}})
	// find the current primary
	initialPrimary := findPrimary(c, session)
	c.Assert(initialPrimary, jc.GreaterThan, -1)
	// ensure the secondaries are up and happy
	// strategy = utils.AttemptStrategy{Total: time.Second, Delay: time.Millisecond * 50}
	attemptLoop(c, strategy, "secondaries are ready", func() error {
		status, err := CurrentStatus(session)
		if err != nil {
			return err
		}
		var notReady []string
		for _, m := range status.Members {
			if m.State != PrimaryState && m.State != SecondaryState {
				notReady = append(notReady, fmt.Sprintf("Member{Id: %d, Address: %s, State: %s}", m.Id, m.Address, m.State.String()))
			}
		}
		if len(notReady) > 0 {
			return errors.Errorf("members not ready: %s", strings.Join(notReady, ", "))
		}
		return nil
	})
	// Now that the secondaries are up, we should be able to ask the primary to step down and notice that the primary changes
	session.SetMode(mgo.Monotonic, true)
	mode := session.Mode()
	err = StepDownPrimary(session)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(session.Mode(), gc.Equals, mode)
	// Changing the primary should cause us to get disconnected, so we need to reconnect
	session.Close()
	session = nil
	attemptLoop(c, strategy, "reconnect", func() error {
		session, err = s.root.Dial()
		if err != nil {
			session = nil
			return err
		}
		return nil
	})
	// Now that we are reconnected, we should definitely have a different primary
	newPrimary := findPrimary(c, session)
	c.Check(newPrimary, gc.Not(gc.Equals), initialPrimary)
}

func (s *MongoSuite) requirePersistentStepdownConnections(c *gc.C, session *mgo.Session) {
	info, err := session.BuildInfo()
	c.Assert(err, jc.ErrorIsNil)
	if !info.VersionAtLeast(4, 2) {
		c.Skip("needs mongo 4.2 or later")
	}
}

// Set must still repair the replicaset once the primary has stepped down on
// losing quorum and mgo no longer knows a primary. As in juju's peergrouper,
// the session keeps the socket its failed reconfig used.
func (s *MongoSuite) TestSetRepairsAfterPrimaryStepsDown(c *gc.C) {
	session := s.root.MustDial()
	defer session.Close()
	s.requirePersistentStepdownConnections(c, session)
	monitor := s.root.MustDialDirect()
	defer monitor.Close()
	monitor.SetMode(mgo.Monotonic, true)
	s1 := newServer(c)
	defer s1.Destroy()
	s2 := newServer(c)
	defer s2.Destroy()
	strategy := utils.AttemptStrategy{Total: time.Minute * 2, Delay: time.Millisecond * 500}
	waitForState := func(id int, state MemberState) {
		attemptLoop(c, strategy, fmt.Sprintf("member %d %s", id, state), func() error {
			status, err := CurrentStatus(monitor)
			if err != nil {
				return err
			}
			for _, m := range status.Members {
				if m.Id == id && m.State == state {
					return nil
				}
			}
			return errors.Errorf("member %d is not %s", id, state)
		})
	}
	attemptLoop(c, strategy, "Add()", func() error {
		return Add(session, Member{Address: s1.Addr()}, Member{Address: s2.Addr()})
	})
	waitForState(1, PrimaryState)
	waitForState(2, SecondaryState)
	waitForState(3, SecondaryState)

	held := false
	readConfig := getSessionConfig
	s.PatchValue(&getSessionConfig, func(rsSession mgoSession) (*Config, error) {
		if !held {
			held = true
			// Hold the repair until the primary has stepped down. A write
			// through a copy of the session, like those of juju's other
			// workers, then fails and makes mgo drop the primary it knew.
			waitForState(1, SecondaryState)
			probe := session.Copy()
			defer probe.Close()
			probe.SetSyncTimeout(time.Second)
			c.Assert(probe.DB("testing").C("probe").Insert(bson.M{}), gc.NotNil)
			c.Assert(probe.Ping(), gc.ErrorMatches, "no reachable servers")
		}
		return readConfig(rsSession)
	})

	// The peergrouper refreshes the session before it calls Set. With two
	// of the three members dead, Set's first remove fails the quorum check.
	session.Refresh()
	s1.Destroy()
	s2.Destroy()
	err := Set(session, []Member{{Id: 1, Address: s.root.Addr(), Tags: initialTags}})
	c.Assert(err, jc.ErrorIsNil)
	c.Check(held, jc.IsTrue)
	waitForState(1, PrimaryState)
	mems, err := CurrentMembers(monitor)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(mems, gc.HasLen, 1)
	assertMembers(c, mems, []Member{{Id: 1, Address: s.root.Addr(), Tags: initialTags}})
}

func (s *MongoSuite) TestSetRetriesAfterAnotherSurvivorBecomesPrimary(c *gc.C) {
	s.checkSetAfterPrimaryChange(c, false, false)
}

func (s *MongoSuite) TestDirectSetCompletesRemovalsAfterPrimaryChanges(c *gc.C) {
	s.checkSetAfterPrimaryChange(c, true, false)
}

func (s *MongoSuite) TestDirectSetCompletesUpdatesAfterPrimaryChanges(c *gc.C) {
	s.PatchValue(&primarySessionAttempts, utils.AttemptStrategy{Total: time.Second, Delay: 100 * time.Millisecond})
	s.checkSetAfterPrimaryChange(c, true, true)
}

func (s *MongoSuite) checkSetAfterPrimaryChange(c *gc.C, direct, updateBeforeFailure bool) {
	dial := s.root.MustDial
	extraNodes := 3
	if direct {
		dial = s.root.MustDialDirect
		// With only two survivors, removing one of five voters still
		// needs a majority of three and must take the repair path.
		extraNodes = 4
	}
	session := dial()
	defer session.Close()
	s.requirePersistentStepdownConnections(c, session)
	nodes := []*mgotesting.MgoInstance{s.root}
	var additions []Member
	for i := 0; i < extraNodes; i++ {
		node := newServer(c)
		defer node.Destroy()
		nodes = append(nodes, node)
		additions = append(additions, Member{Address: node.Addr()})
	}
	strategy := utils.AttemptStrategy{Total: time.Minute, Delay: 100 * time.Millisecond}
	attemptLoop(c, strategy, "add three members", func() error {
		return Add(session, additions...)
	})
	a := nodes[0].MustDialDirect()
	defer a.Close()
	b := nodes[1].MustDialDirect()
	defer b.Close()
	a.SetMode(mgo.Monotonic, true)
	b.SetMode(mgo.Monotonic, true)
	state := func(observer *mgo.Session, id int, wanted MemberState) error {
		status, err := CurrentStatus(observer)
		if err != nil {
			return err
		}
		for _, member := range status.Members {
			if member.Id == id && member.State == wanted {
				return nil
			}
		}
		return fmt.Errorf("member %d has not reached %s", id, wanted)
	}
	for id := 1; id <= len(nodes); id++ {
		wanted := MemberState(SecondaryState)
		if id == 1 {
			wanted = PrimaryState
		}
		id, wanted := id, wanted
		attemptLoop(c, strategy, "initial member state", func() error {
			return state(a, id, wanted)
		})
	}

	readConfig := getSessionConfig
	readStatus := getCurrentStatus
	frozen, promoted := false, false
	s.PatchValue(&getSessionConfig, func(rsSession mgoSession) (*Config, error) {
		if !frozen {
			attemptLoop(c, strategy, "member 1 steps down after losing quorum", func() error {
				return state(a, 1, SecondaryState)
			})
			c.Assert(a.Run(bson.D{{Name: "replSetFreeze", Value: 60}}, nil), jc.ErrorIsNil)
			frozen = true
		} else if promoted {
			result, err := IsMaster(rsSession)
			c.Assert(err, jc.ErrorIsNil)
			c.Check(result.IsMaster, gc.Equals, !direct)
			if direct {
				c.Check(result.Address, gc.Equals, nodes[0].Addr())
			} else {
				c.Check(result.Address, gc.Equals, nodes[1].Addr())
			}
		}
		return readConfig(rsSession)
	})
	s.PatchValue(&getCurrentStatus, func(rsSession mgoSession) (*Status, error) {
		status, err := readStatus(rsSession)
		if err != nil || !frozen || promoted || len(status.Members) != 2 {
			return status, err
		}
		attemptLoop(c, strategy, "forced config reaches member 2", func() error {
			config, err := readConfig(b)
			if err != nil {
				return err
			}
			if len(config.Members) != 2 {
				return fmt.Errorf("member 2 still has %d members", len(config.Members))
			}
			return nil
		})
		c.Assert(b.Run("replSetStepUp", nil), jc.ErrorIsNil)
		attemptLoop(c, strategy, "member 2 elected primary", func() error {
			return state(a, 2, PrimaryState)
		})
		c.Assert(state(a, 1, SecondaryState), jc.ErrorIsNil)
		promoted = true
		return readStatus(rsSession)
	})

	session.Refresh()
	stopMembers := func() {
		for _, node := range nodes[2:] {
			node.Destroy()
		}
	}
	wantedAddress := nodes[1].Addr()
	if !direct || updateBeforeFailure {
		wantedAddress = strings.Replace(wantedAddress, "localhost:", "127.0.0.1:", 1)
	}
	var changeSession mgoSession = session
	if updateBeforeFailure {
		changeSession = &afterReconfigSession{mgoSession: session, after: stopMembers}
	} else {
		stopMembers()
	}
	err := Set(changeSession, []Member{
		{Id: 1, Address: nodes[0].Addr(), Tags: initialTags},
		{Id: 2, Address: wantedAddress},
	})
	c.Check(frozen, jc.IsTrue)
	c.Check(promoted, jc.IsTrue)
	config, configErr := readConfig(b)
	c.Assert(configErr, jc.ErrorIsNil)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(config.Members, gc.HasLen, 2)
	c.Check(config.Members[0].Id, gc.Equals, 1)
	c.Check(config.Members[1].Id, gc.Equals, 2)
	c.Assert(config.Members[1].Address, gc.Equals, wantedAddress)
	original, err := IsMaster(session)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(original.Secondary, jc.IsTrue)
	c.Check(original.Address, gc.Equals, nodes[0].Addr())
}

type afterReconfigSession struct {
	mgoSession
	after func()
}

func (s *afterReconfigSession) Run(cmd interface{}, result interface{}) error {
	err := s.mgoSession.Run(cmd, result)
	if data, ok := cmd.(bson.D); err == nil && ok && data[0].Name == "replSetReconfig" && s.after != nil {
		after := s.after
		s.after = nil
		after()
	}
	return err
}

func (s *MongoSuite) TestPrimarySessionKeepsOriginalOpen(c *gc.C) {
	original := s.root.MustDialDirect()
	defer original.Close()
	primary, err := primarySession(original)
	c.Assert(err, jc.ErrorIsNil)
	primary.Close()
	c.Check(original.Ping(), jc.ErrorIsNil)
}

func (s *MongoSuite) TestPrimarySessionPreservesTLSAndAuth(c *gc.C) {
	certPEM, keyPEM, err := cert.NewLeaf(&cert.Config{
		CommonName: "anything", Hostnames: []string{"anything", "localhost"},
		IsCA: true, Expiry: time.Now().Add(time.Hour),
	})
	c.Assert(err, jc.ErrorIsNil)
	serverCert, serverKey, err := cert.ParseCertAndKey(certPEM, keyPEM)
	c.Assert(err, jc.ErrorIsNil)
	certs := &mgotesting.Certs{
		CACert:     serverCert,
		ServerCert: serverCert,
		ServerKey:  serverKey,
	}
	inst := &mgotesting.MgoInstance{
		Params:     []string{"--replSet", rsName},
		EnableAuth: true,
	}
	c.Assert(inst.Start(certs), jc.ErrorIsNil)
	defer inst.Destroy()
	dialInfo := mgotesting.MgoDialInfo(certs, inst.Addr())
	directInfo := *dialInfo
	directInfo.Direct = true
	original, err := mgo.DialWithInfo(&directInfo)
	c.Assert(err, jc.ErrorIsNil)
	defer original.Close()
	original.SetMode(mgo.Monotonic, true)
	c.Assert(original.Run(bson.D{{Name: "replSetInitiate", Value: Config{
		Name: rsName, Version: 1, Members: []Member{{Id: 1, Address: inst.Addr()}},
	}}}, nil), jc.ErrorIsNil)
	c.Assert(waitForPrimary(original), jc.ErrorIsNil)
	credential := &mgo.Credential{Source: "admin", Username: "admin", Password: "test-password"}
	c.Assert(original.DB("admin").UpsertUser(&mgo.User{
		Username: credential.Username, Password: credential.Password, Roles: []mgo.Role{mgo.RoleRoot},
	}), jc.ErrorIsNil)
	c.Assert(original.Login(credential), jc.ErrorIsNil)
	primary, err := primarySession(original)
	c.Assert(err, jc.ErrorIsNil)
	defer primary.Close()
	c.Assert(primary.DB("test").C("authenticated").Insert(bson.M{"value": 1}), jc.ErrorIsNil)
	c.Check(original.Ping(), jc.ErrorIsNil)
}

// The mgo test harness binds mongod to localhost, which may resolve only to
// 127.0.0.1, so the IPv6 tests bind to all addresses to reach [::1].
var ipv6Params = []string{"--bind_ip_all"}

func ipv6GetAddr(inst *mgotesting.MgoInstance) string {
	return fmt.Sprintf("[::1]:%v", inst.Port())
}

type MongoIPV6Suite struct {
	testing.IsolationSuite
}

var _ = gc.Suite(&MongoIPV6Suite{})

func (s *MongoIPV6Suite) SetUpTest(c *gc.C) {
	s.IsolationSuite.SetUpTest(c)
	s.PatchEnvironment("JUJU_MONGO_STORAGE_ENGINE", "wiredTiger")
}

func (s *MongoIPV6Suite) TestAddRemoveSetIPv6(c *gc.C) {
	root := newServer(c, ipv6Params...)
	defer root.Destroy()
	dialAndTestInitiate(c, root, ipv6GetAddr(root))
	assertAddRemoveSet(c, root, ipv6GetAddr, ipv6Params...)
}

func (s *MongoIPV6Suite) TestAddressFixing(c *gc.C) {
	root := newServer(c, ipv6Params...)
	defer root.Destroy()
	dialAndTestInitiate(c, root, ipv6GetAddr(root))
	session := root.MustDial()
	defer session.Close()

	status, err := CurrentStatus(session)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(len(status.Members), jc.DeepEquals, 1)
	c.Check(status.Members[0].Address, gc.Equals, ipv6GetAddr(root))

	cfg, err := CurrentConfig(session)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(len(cfg.Members), jc.DeepEquals, 1)
	c.Check(cfg.Members[0].Address, gc.Equals, ipv6GetAddr(root))

	result, err := IsMaster(session)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(result.Address, gc.Equals, ipv6GetAddr(root))
	c.Check(result.PrimaryAddress, gc.Equals, ipv6GetAddr(root))
	c.Check(result.Addresses, jc.DeepEquals, []string{ipv6GetAddr(root)})
}

type mockSession struct {
	mgoSession
	cfg         *Config
	repaired    bool
	reconfigErr error
	// failWhen selects which reconfig loses quorum; when nil the remove
	// leaving only member 666 fails.
	failWhen      func(cfg Config) bool
	retryFailWhen func(cfg Config) bool
	// forceErr is returned once a forced reconfig is applied.
	forceErr  error
	refreshes int
}

func (m *mockSession) Run(cmd interface{}, _ interface{}) error {
	data, ok := cmd.(bson.D)
	if !ok {
		return fmt.Errorf("unexpected cmd data %v", cmd)
	}
	if len(data) < 1 || data[0].Name != "replSetReconfig" {
		return fmt.Errorf("unexpected cmd data %v", data)
	}
	cfg, ok := data[0].Value.(Config)
	if !ok {
		return fmt.Errorf("unexpected cmd data %v", data[0].Value)
	}
	forced := len(data) > 1
	if forced {
		force, ok := data[1].Value.(bool)
		if !ok || !force {
			return fmt.Errorf("unexpected force value %v", data[1].Value)
		}
		m.repaired = true
	}
	failWhen := m.failWhen
	if failWhen == nil {
		failWhen = func(cfg Config) bool {
			return len(cfg.Members) == 1 && cfg.Members[0].Id == 666
		}
	}
	initialFailure := !m.repaired && failWhen(cfg)
	retryFailure := m.repaired && !forced && m.retryFailWhen != nil && m.retryFailWhen(cfg)
	if initialFailure || retryFailure {
		if m.reconfigErr != nil {
			return m.reconfigErr
		}
		return &mgo.QueryError{Code: 11602, Message: "Quorum check failed because not enough voting nodes responded"}
	}
	// Copy the members: the caller keeps updating the same slice in place,
	// and a later rejected reconfig must not change the stored config.
	cfg.Members = append([]Member(nil), cfg.Members...)
	m.cfg = &cfg
	if forced {
		return m.forceErr
	}
	return nil
}

func (*mockSession) Ping() error { return nil }
func (m *mockSession) Refresh()  { m.refreshes++ }
func (*mockSession) Close()      {}

type changesSuite struct {
	testing.IsolationSuite

	current []Member
}

var _ = gc.Suite(&changesSuite{})

func (s *changesSuite) SetUpTest(c *gc.C) {
	currentConfig := func(session mgoSession) (*Config, error) {
		return &Config{
			Members: append([]Member(nil), s.current...),
		}, nil
	}
	s.PatchValue(&getCurrentConfig, currentConfig)
	s.PatchValue(&getSessionConfig, currentConfig)
	s.PatchValue(&getPrimarySession, func(session mgoSession) (mgoSession, error) {
		return session, nil
	})
	s.PatchValue(&confirmDeadInterval, time.Duration(0))
	s.PatchValue(&primaryPollInterval, time.Duration(0))
}

type primaryCheckSession struct {
	mgoSession
	check     func(*IsMasterResults) error
	refreshes int
}

func (s *primaryCheckSession) Run(_ interface{}, result interface{}) error {
	return s.check(result.(*IsMasterResults))
}

func (s *primaryCheckSession) Refresh() { s.refreshes++ }

func (s *changesSuite) TestWaitForPrimaryReselectsSecondary(c *gc.C) {
	s.PatchValue(&primarySessionAttempts, utils.AttemptStrategy{Min: 2})
	calls := 0
	session := &primaryCheckSession{check: func(result *IsMasterResults) error {
		calls++
		result.IsMaster = calls == 2
		return nil
	}}
	c.Assert(waitForPrimary(session), jc.ErrorIsNil)
	c.Check(calls, gc.Equals, 2)
	c.Check(session.refreshes, gc.Equals, 1)
}

func (s *changesSuite) TestWaitForPrimaryStopsOnSecondary(c *gc.C) {
	s.PatchValue(&primarySessionAttempts, utils.AttemptStrategy{Min: 2})
	session := &primaryCheckSession{check: func(result *IsMasterResults) error {
		result.Address = "secondary:27017"
		return nil
	}}
	c.Assert(waitForPrimary(session), gc.ErrorMatches, `cannot select primary after replicaset repair: server "secondary:27017" is not primary`)
	c.Check(session.refreshes, gc.Equals, 2)
}

func (s *changesSuite) TestWaitForPrimaryRetriesQueryError(c *gc.C) {
	s.PatchValue(&primarySessionAttempts, utils.AttemptStrategy{Min: 2})
	calls := 0
	session := &primaryCheckSession{check: func(result *IsMasterResults) error {
		calls++
		if calls == 1 {
			return errors.New("no reachable servers")
		}
		result.IsMaster = true
		return nil
	}}
	c.Assert(waitForPrimary(session), jc.ErrorIsNil)
	c.Check(calls, gc.Equals, 2)
	c.Check(session.refreshes, gc.Equals, 1)
}

func (s *changesSuite) TestSetNoChanges(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}}
	m := &mockSession{}
	wantMembers := []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, gc.IsNil)
}

var (
	one     = 1.0
	votes   = 1
	novotes = 0
)

func (s *changesSuite) TestSetAdds(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}}
	m := &mockSession{}
	wantMembers := []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 1,
		Members: []Member{{
			Id:      1,
			Address: "10.0.0.1",
		}, {
			Id:       2,
			Address:  "10.0.0.2",
			Priority: &one,
			Votes:    &votes,
		}},
	})
}

func (s *changesSuite) TestSetUpdateAddress(c *gc.C) {
	s.current = []Member{{
		Id:      4,
		Address: "10.0.0.1",
	}}
	m := &mockSession{}
	wantMembers := []Member{{
		Id:      5,
		Address: "10.0.0.3",
	}, {
		Id:      4,
		Address: "10.0.0.2",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 2,
		Members: []Member{{
			Id:      4,
			Address: "10.0.0.2",
		}, {
			Id:       5,
			Address:  "10.0.0.3",
			Priority: &one,
			Votes:    &votes,
		}},
	})
}

func (s *changesSuite) TestSetUpdateVote(c *gc.C) {
	s.current = []Member{{
		Id:       4,
		Address:  "10.0.0.2",
		Priority: &one,
		Votes:    &votes,
	}, {
		Id:      5,
		Address: "10.0.0.3",
		Votes:   &novotes,
	}}
	m := &mockSession{}
	wantMembers := []Member{{
		Id:       5,
		Address:  "10.0.0.3",
		Priority: &one,
		Votes:    &votes,
	}, {
		Id:      4,
		Address: "10.0.0.2",
		Votes:   &novotes,
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 2,
		Members: []Member{{
			Id:      4,
			Address: "10.0.0.2",
			Votes:   &novotes,
		}, {
			Id:       5,
			Address:  "10.0.0.3",
			Priority: &one,
			Votes:    &votes,
		}},
	})
}

func (s *changesSuite) TestSetUpdatesMemberAtExistingAddress(c *gc.C) {
	s.current = []Member{{
		Id:       4,
		Address:  "10.0.0.2",
		Priority: &one,
		Votes:    &votes,
	}, {
		Id:       5,
		Address:  "10.0.0.3",
		Priority: &one,
		Votes:    &votes,
	}}
	m := &mockSession{}
	wantMembers := []Member{{
		Id:       5,
		Address:  "10.0.0.3",
		Priority: &one,
		Votes:    &votes,
	}, {
		// No Id: the member already at this address must be updated,
		// not added again.
		Address: "10.0.0.2",
		Votes:   &novotes,
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 1,
		Members: []Member{{
			Id:      4,
			Address: "10.0.0.2",
			Votes:   &novotes,
		}, {
			Id:       5,
			Address:  "10.0.0.3",
			Priority: &one,
			Votes:    &votes,
		}},
	})
}

func (s *changesSuite) TestSetNoChangesForMemberAtExistingAddress(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}}
	m := &mockSession{}
	wantMembers := []Member{{
		Address: "10.0.0.1",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, gc.IsNil)
}

func (s *changesSuite) TestSetErrorsForOtherIdAtExistingAddress(c *gc.C) {
	s.current = []Member{{
		Id:      4,
		Address: "10.0.0.2",
	}}
	m := &mockSession{}
	wantMembers := []Member{{
		Id:      6,
		Address: "10.0.0.2",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, gc.ErrorMatches, `cannot set member 6: address "10.0.0.2" already used by member 4`)
	c.Assert(m.cfg, gc.IsNil)
}

func (s *changesSuite) TestSetAddsAtAddressOfMovedMember(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}}
	m := &mockSession{}
	wantMembers := []Member{{
		Id:      1,
		Address: "10.0.0.9",
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}, {
		// No Id: member 1 moves away, so its old address is free for a
		// new member.
		Address: "10.0.0.1",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 2,
		Members: []Member{{
			Id:      1,
			Address: "10.0.0.9",
		}, {
			Id:      2,
			Address: "10.0.0.2",
		}, {
			Id:       3,
			Address:  "10.0.0.1",
			Priority: &one,
			Votes:    &votes,
		}},
	})
}

func (s *changesSuite) TestSetUpdatesMemberAtLocalhostAddress(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "127.0.0.1:37017",
	}, {
		Id:      2,
		Address: "10.0.0.2:37017",
	}}
	m := &mockSession{}
	wantMembers := []Member{{
		Id:      1,
		Address: "localhost:37017",
	}, {
		Id:      2,
		Address: "10.0.0.2:37017",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 1,
		Members: []Member{{
			Id:      1,
			Address: "localhost:37017",
		}, {
			Id:      2,
			Address: "10.0.0.2:37017",
		}},
	})
}

func (s *changesSuite) TestSetRemovesMemberZeroWhenIdOmitted(c *gc.C) {
	s.current = []Member{{
		Id:      0,
		Address: "10.0.0.1",
	}, {
		Id:      1,
		Address: "10.0.0.2",
	}}
	m := &mockSession{}
	// No Id: the wanted member is member 1 by address, so member 0 is
	// no longer wanted and must be removed.
	wantMembers := []Member{{
		Address: "10.0.0.2",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 1,
		Members: []Member{{
			Id:      1,
			Address: "10.0.0.2",
		}},
	})
}

func (s *changesSuite) TestSetAddsMembersWithoutIdBesideMemberZero(c *gc.C) {
	s.current = []Member{{
		Id:      0,
		Address: "10.0.0.1",
	}, {
		Id:      1,
		Address: "10.0.0.2",
	}}
	m := &mockSession{}
	// The two members without an Id match no existing address, so both are
	// new, and member 0 is no longer wanted.
	err := Set(m, []Member{{
		Id:      1,
		Address: "10.0.0.2",
	}, {
		Address: "10.0.0.3",
	}, {
		Address: "10.0.0.4",
	}})
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 3,
		Members: []Member{{
			Id:      1,
			Address: "10.0.0.2",
		}, {
			Id:       2,
			Address:  "10.0.0.3",
			Priority: &one,
			Votes:    &votes,
		}, {
			Id:       3,
			Address:  "10.0.0.4",
			Priority: &one,
			Votes:    &votes,
		}},
	})
}

func (s *changesSuite) TestSetUpdatesMemberZeroWhenIdOmitted(c *gc.C) {
	s.current = []Member{{
		Id:      0,
		Address: "10.0.0.1",
	}, {
		Id:      1,
		Address: "10.0.0.2",
	}}
	m := &mockSession{}
	// No Id, but the address is member 0's, so member 0 is updated.
	err := Set(m, []Member{{
		Address: "10.0.0.1",
		Votes:   &novotes,
	}, {
		Id:      1,
		Address: "10.0.0.2",
	}})
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 1,
		Members: []Member{{
			Id:      0,
			Address: "10.0.0.1",
			Votes:   &novotes,
		}, {
			Id:      1,
			Address: "10.0.0.2",
		}},
	})
}

func (s *changesSuite) TestSetSendsMembersAtTheSameAddress(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}}
	m := &mockSession{}
	// MongoDB rejects two members at one address. Set must send member 3
	// rather than drop it.
	err := Set(m, []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}, {
		Id:      3,
		Address: "10.0.0.2",
	}})
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 2,
		Members: []Member{{
			Id:      1,
			Address: "10.0.0.1",
		}, {
			Id:       2,
			Address:  "10.0.0.2",
			Priority: &one,
			Votes:    &votes,
		}, {
			Id:       3,
			Address:  "10.0.0.2",
			Priority: &one,
			Votes:    &votes,
		}},
	})
}

func (s *changesSuite) TestSetAddsLocalhostMemberOnAnotherPort(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "127.0.0.1:37017",
	}}
	m := &mockSession{}
	err := Set(m, []Member{{
		Id:      1,
		Address: "127.0.0.1:37017",
	}, {
		Address: "127.0.0.1:37018",
	}})
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 1,
		Members: []Member{{
			Id:      1,
			Address: "127.0.0.1:37017",
		}, {
			Id:       2,
			Address:  "127.0.0.1:37018",
			Priority: &one,
			Votes:    &votes,
		}},
	})
}

func (s *changesSuite) TestSetRemoves(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}}
	m := &mockSession{}
	wantMembers := []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 1,
		Members: []Member{{
			Id:      1,
			Address: "10.0.0.1",
		}},
	})
}

func (s *changesSuite) TestSetUpdateAndRemoves(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}}
	m := &mockSession{}
	wantMembers := []Member{{
		Id:      1,
		Address: "10.0.0.3",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 2,
		Members: []Member{{
			Id:      1,
			Address: "10.0.0.3",
		}},
	})
}

func (s *changesSuite) TestRepair(c *gc.C) {
	for _, test := range []struct {
		pendingRemoval bool
		completedAdd   bool
		readError      bool
	}{
		{},
		{pendingRemoval: true},
		{completedAdd: true},
		{readError: true},
	} {
		c.Logf("repair case: %+v", test)
		s.setupRepairStatus(c)
		s.current = []Member{{
			Id:      1,
			Address: "10.0.0.1",
		}, {
			Id:      666,
			Address: "10.0.0.2",
		}}
		wantVersion := 1
		wantSelections := 0
		if test.pendingRemoval {
			s.current = append(s.current, Member{Id: 667, Address: "10.0.0.3"})
			wantVersion = 2
			wantSelections = 1
		}
		m := &mockSession{
			failWhen: func(cfg Config) bool {
				if test.completedAdd {
					return len(cfg.Members) == 2
				}
				return len(cfg.Members) < len(s.current)
			},
		}
		s.patchConfigFromSession(c, m)
		if test.readError {
			readConfig := getSessionConfig
			reads := 0
			s.PatchValue(&getSessionConfig, func(session mgoSession) (*Config, error) {
				reads++
				if reads == 2 {
					return nil, errors.New("lost the repair connection")
				}
				return readConfig(session)
			})
			wantSelections = 1
		}
		selections := 0
		s.PatchValue(&getPrimarySession, func(session mgoSession) (mgoSession, error) {
			selections++
			return session, nil
		})
		wantMembers := []Member{{
			Id:      666,
			Address: "10.0.0.2",
		}}
		if test.completedAdd {
			one := 1.0
			votes := 1
			wantMembers = append(wantMembers, Member{
				Id: 667, Address: "10.0.0.3", Priority: &one, Votes: &votes,
			})
			wantVersion = 2
		}
		err := Set(m, wantMembers)
		c.Assert(err, jc.ErrorIsNil)
		c.Assert(m.cfg, jc.DeepEquals, &Config{
			Version: wantVersion,
			Members: wantMembers,
		})
		c.Check(selections, gc.Equals, wantSelections)
		c.Assert(m.repaired, jc.IsTrue)
	}
}

func (s *changesSuite) TestRepairQuorumCheckFailureMessage(c *gc.C) {
	s.setupRepairStatus(c)

	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      666,
		Address: "10.0.0.2",
	}}
	m := &mockSession{
		reconfigErr: &mgo.QueryError{
			Code:    74,
			Message: "Quorum check failed because not enough voting nodes responded",
		},
	}
	s.patchConfigFromSession(c, m)
	wantMembers := []Member{{
		Id:      666,
		Address: "10.0.0.2",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 1,
		Members: []Member{{
			Id:      666,
			Address: "10.0.0.2",
		}},
	})
	c.Assert(m.repaired, jc.IsTrue)
}

func (s *changesSuite) TestRepairDuringUpdate(c *gc.C) {
	s.setupRepairStatus(c)

	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      666,
		Address: "10.0.0.2",
	}}
	m := &mockSession{
		// Fail the update reconfig, before the remove step is reached.
		failWhen: func(cfg Config) bool { return len(cfg.Members) == 2 },
	}
	s.patchConfigFromSession(c, m)
	wantMembers := []Member{{
		Id:      666,
		Address: "10.0.0.3",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 2,
		Members: []Member{{
			Id:      666,
			Address: "10.0.0.3",
		}},
	})
	c.Assert(m.repaired, jc.IsTrue)
}

func (s *changesSuite) TestRepairReadsConfigThroughSession(c *gc.C) {
	s.setupRepairStatus(c)
	s.current = []Member{{Id: 1, Address: "10.0.0.1"}, {Id: 666, Address: "10.0.0.2"}}
	m := &mockSession{}
	s.patchConfigFromSession(c, m)
	// Only Set's first read may use getCurrentConfig. A later read may come
	// from a secondary that has not seen the forced reconfig.
	reads := 0
	s.PatchValue(&getCurrentConfig, func(session mgoSession) (*Config, error) {
		reads++
		if reads > 1 {
			return nil, errors.New("read from a secondary")
		}
		return &Config{Members: append([]Member(nil), s.current...)}, nil
	})
	err := Set(m, []Member{{Id: 666, Address: "10.0.0.2"}})
	c.Assert(err, jc.ErrorIsNil)
	c.Check(m.repaired, jc.IsTrue)
	c.Check(m.cfg.Members, jc.DeepEquals, []Member{{Id: 666, Address: "10.0.0.2"}})
}

func (s *changesSuite) TestRepairCompletesAfterUpdate(c *gc.C) {
	s.setupRepairStatus(c)
	s.current = []Member{{Id: 1, Address: "10.0.0.1"}, {Id: 666, Address: "10.0.0.2"}}
	m := &mockSession{}
	s.patchConfigFromSession(c, m)
	s.PatchValue(&getPrimarySession, func(mgoSession) (mgoSession, error) {
		c.Error("completed changes must not select a primary")
		return nil, errors.New("primary unavailable")
	})
	want := []Member{{Id: 666, Address: "10.0.0.3"}}
	c.Assert(Set(m, want), jc.ErrorIsNil)
	c.Check(m.repaired, jc.IsTrue)
	c.Check(m.cfg.Members, jc.DeepEquals, want)
}

func (s *changesSuite) TestRepairRefreshesAfterForcedReconfigEOF(c *gc.C) {
	s.setupRepairStatus(c)
	s.current = []Member{{Id: 1, Address: "10.0.0.1"}, {Id: 666, Address: "10.0.0.2"}}
	// The forced reconfig is applied, then the primary steps down and drops
	// the connection.
	m := &mockSession{forceErr: io.EOF}
	s.patchConfigFromSession(c, m)
	err := Set(m, []Member{{Id: 666, Address: "10.0.0.2"}})
	c.Assert(err, jc.ErrorIsNil)
	c.Check(m.repaired, jc.IsTrue)
	c.Check(m.refreshes, gc.Equals, 1)
	c.Check(m.cfg.Members, jc.DeepEquals, []Member{{Id: 666, Address: "10.0.0.2"}})
}

func (s *changesSuite) TestRepairKeepsWaitingAfterStatusError(c *gc.C) {
	s.setupRepairStatus(c)
	s.current = []Member{{Id: 1, Address: "10.0.0.1"}, {Id: 666, Address: "10.0.0.2"}}
	m := &mockSession{}
	s.patchConfigFromSession(c, m)
	status := getCurrentStatus
	calls := 0
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		if !m.repaired {
			return status(session)
		}
		// Fail the first status check after the forced reconfig.
		calls++
		if calls == 1 {
			return nil, errors.New("connection reset")
		}
		return status(session)
	})
	err := Set(m, []Member{{Id: 666, Address: "10.0.0.2"}})
	c.Assert(err, jc.ErrorIsNil)
	c.Check(calls, gc.Equals, 2)
	c.Check(m.repaired, jc.IsTrue)
	c.Check(m.refreshes, gc.Equals, 1)
}

func (s *changesSuite) TestRepairReturnsLastStatusError(c *gc.C) {
	s.setupRepairStatus(c)
	s.current = []Member{{Id: 1, Address: "10.0.0.1"}, {Id: 666, Address: "10.0.0.2"}}
	m := &mockSession{}
	s.patchConfigFromSession(c, m)
	status := getCurrentStatus
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		if m.repaired {
			return nil, errors.New("connection reset")
		}
		return status(session)
	})
	err := Set(m, []Member{{Id: 666, Address: "10.0.0.2"}})
	c.Assert(err, gc.ErrorMatches, `repairing replicaset after .*: checking rs status to repair: connection reset`)
	c.Check(m.refreshes, gc.Equals, primaryPollAttempts)
}

func (s *changesSuite) TestRepairReturnsContextWhenRetryLosesQuorum(c *gc.C) {
	// Report member 1 as persistently down and member 666 as healthy.
	s.setupRepairStatus(c)

	// Start with the dead removal target and the member whose address changes.
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      666,
		Address: "10.0.0.2",
	}}

	// Lose quorum on the initial update and again when it is retried after
	// repair.
	m := &mockSession{
		failWhen:      func(cfg Config) bool { return len(cfg.Members) == 2 },
		retryFailWhen: func(cfg Config) bool { return len(cfg.Members) == 1 },
	}

	// Make the retry load the configuration written by the forced repair.
	s.patchConfigFromSession(c, m)

	// Request the surviving member's address update while removing the dead
	// member.
	wantMembers := []Member{{
		Id:      666,
		Address: "10.0.0.3",
	}}
	err := Set(m, wantMembers)

	// Return useful public context instead of exposing the internal sentinel.
	c.Assert(err, gc.ErrorMatches, `cannot apply Set changes after replicaset repair attempt: cannot update member .*Id:666.* in replicaset: repair needed: Quorum check failed because not enough voting nodes responded`)
	c.Assert(errors.Is(err, repairNeeded), jc.IsFalse)
	c.Assert(m.repaired, jc.IsTrue)
}

func (s *changesSuite) TestRepairDuringMultipleUpdates(c *gc.C) {
	// Configure one dead removal target and two members whose addresses change.
	s.setupRepairStatus(c)
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      666,
		Address: "10.0.0.2",
	}, {
		Id:      667,
		Address: "10.0.0.3",
	}}

	// Let the first update succeed and make the second update lose quorum.
	m := &mockSession{}
	m.failWhen = func(cfg Config) bool {
		if cfg.Members[1].Address != "10.0.0.4" || cfg.Members[2].Address != "10.0.0.5" {
			return false
		}
		// The rejected update must not show in the stored config.
		c.Check(m.cfg.Members[2].Address, gc.Equals, "10.0.0.3")
		return true
	}
	// Return the last successful config when repair and retry reload it.
	s.patchConfigFromSession(c, m)

	// Request both address updates while omitting the dead member.
	wantMembers := []Member{{
		Id:      666,
		Address: "10.0.0.4",
	}, {
		Id:      667,
		Address: "10.0.0.5",
	}}
	err := Set(m, wantMembers)

	// The retry must preserve the first update and apply the second one.
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 4,
		Members: []Member{{
			Id:      666,
			Address: "10.0.0.4",
		}, {
			Id:      667,
			Address: "10.0.0.5",
		}},
	})
	// A forced reconfig must have removed the dead target before the retry.
	c.Assert(m.repaired, jc.IsTrue)
}

func (s *changesSuite) TestRepairDuringAdd(c *gc.C) {
	s.setupRepairStatus(c)

	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      666,
		Address: "10.0.0.2",
	}}
	m := &mockSession{
		// The first add succeeds, the second one loses quorum. The re-drive
		// must skip the already added member and the already removed one.
		failWhen: func(cfg Config) bool { return len(cfg.Members) == 4 },
	}
	s.patchConfigFromSession(c, m)
	wantMembers := []Member{{
		Id:      666,
		Address: "10.0.0.2",
	}, {
		Id:      667,
		Address: "10.0.0.3",
	}, {
		Id:      668,
		Address: "10.0.0.4",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, jc.DeepEquals, &Config{
		Version: 3,
		Members: []Member{{
			Id:      666,
			Address: "10.0.0.2",
		}, {
			Id:       667,
			Address:  "10.0.0.3",
			Priority: &one,
			Votes:    &votes,
		}, {
			Id:       668,
			Address:  "10.0.0.4",
			Priority: &one,
			Votes:    &votes,
		}},
	})
	c.Assert(m.repaired, jc.IsTrue)
}

func (s *changesSuite) TestRetrySkipsAddStoredWithDefaultPort(c *gc.C) {
	m := &mockSession{}
	// MongoDB stores an added address with its port, so the retry reads
	// member 2 back as localhost:27017.
	_, err := doReplSetConfigChanges("Add", m, &Config{
		Members: []Member{{
			Id:      1,
			Address: "127.0.0.1:27017",
		}, {
			Id:      2,
			Address: "localhost:27017",
		}},
	}, nil, []Member{{
		Id:      2,
		Address: "localhost",
	}}, nil)
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.cfg, gc.IsNil)
}

func (s *changesSuite) TestChangesKeepDistinctLoopbackMembers(c *gc.C) {
	members := []Member{
		{Id: 1, Address: "127.0.0.1:37017"},
		{Id: 2, Address: "127.1.0.1:37017"},
		{Id: 3, Address: "127.2.0.1:37017"},
	}
	newMember := Member{Address: "127.3.0.1:37017"}
	for _, test := range []struct {
		name  string
		apply func(mgoSession) error
		ids   []int
		addrs []string
	}{{
		name: "Add",
		apply: func(session mgoSession) error {
			return Add(session, newMember)
		},
		ids:   []int{1, 2, 3, 4},
		addrs: []string{members[0].Address, members[1].Address, members[2].Address, newMember.Address},
	}, {
		name: "Set",
		apply: func(session mgoSession) error {
			wanted := append([]Member(nil), members...)
			return Set(session, append(wanted, newMember))
		},
		ids:   []int{1, 2, 3, 4},
		addrs: []string{members[0].Address, members[1].Address, members[2].Address, newMember.Address},
	}, {
		name: "Remove",
		apply: func(session mgoSession) error {
			return Remove(session, members[1].Address)
		},
		ids:   []int{1, 3},
		addrs: []string{members[0].Address, members[2].Address},
	}} {
		c.Log(test.name)
		s.current = members
		m := &mockSession{}
		c.Assert(test.apply(m), jc.ErrorIsNil)
		if !c.Check(m.cfg, gc.NotNil) {
			continue
		}
		var ids []int
		var addrs []string
		for _, member := range m.cfg.Members {
			ids = append(ids, member.Id)
			addrs = append(addrs, member.Address)
		}
		c.Check(ids, jc.DeepEquals, test.ids)
		c.Check(addrs, jc.DeepEquals, test.addrs)
		c.Check(m.repaired, jc.IsFalse)
	}
}

func (s *changesSuite) TestChangesKeepLocalhostSeparate(c *gc.C) {
	s.current = []Member{
		{Id: 1, Address: "127.1.0.1:37017"},
		{Id: 2, Address: "127.2.0.1:37017"},
	}
	local := Member{Id: 3, Address: "localhost:37017"}
	m := &mockSession{}
	c.Assert(Add(m, local), jc.ErrorIsNil)
	c.Assert(m.cfg, gc.NotNil)
	c.Check(m.cfg.Members, jc.DeepEquals, append(s.current, local))

	m = &mockSession{}
	c.Assert(Set(m, append(s.current, local)), jc.ErrorIsNil)
	c.Assert(m.cfg, gc.NotNil)
	c.Check(m.cfg.Members[2].Id, gc.Equals, local.Id)
	c.Check(m.cfg.Members[2].Address, gc.Equals, local.Address)

	s.current = []Member{s.current[0], local, s.current[1]}
	m = &mockSession{}
	c.Assert(Remove(m, local.Address), jc.ErrorIsNil)
	c.Assert(m.cfg, gc.NotNil)
	c.Check(m.cfg.Members, jc.DeepEquals, []Member{s.current[0], s.current[2]})
	c.Check(m.repaired, jc.IsFalse)
}

func (s *changesSuite) TestRemoveDoesNotMatchUnknownLocalhost(c *gc.C) {
	s.current = []Member{{Id: 1, Address: "127.0.0.1:37017"}}
	m := &mockSession{}
	c.Assert(Remove(m, "localhost:37017"), jc.ErrorIsNil)
	c.Check(m.cfg, gc.IsNil)
}

func (s *changesSuite) TestSameAddress(c *gc.C) {
	for i, test := range []struct {
		a, b string
		want bool
	}{
		{"127.0.0.1", "localhost", false},
		{"127.0.0.1", "127.0.0.1:27017", true},
		{"127.0.0.1", "127.0.0.1:37017", false},
		{"localhost", "localhost:37017", false},
		{"127.0.0.1:37017", "127.0.0.2:37017", false},
		{"127.0.0.1:37017", "127.1.0.1:37017", false},
		{"127.0.0.1:37017", "[::ffff:127.0.0.1]:37017", true},
		{"127.0.0.1:37017", "[::ffff:127.1.0.1]:37017", false},
		{"[::1]:37017", "[::ffff:127.0.0.1]:37017", false},
		{"127.0.0.1:37017", "127.0.0.1:37018", false},
		{"127.0.0.1:37017", "localhost:37017", false},
		{"[::1]:37017", "127.0.0.1:37017", false},
		{"127.1.2.3:37017", "localhost:37017", false},
		{"127.0.0.1.example.com:37017", "localhost:37017", false},
		{"[::1]", "localhost:27017", false},
		{"[::1]", "localhost:37017", false},
		{"[fd00::1]", "localhost", false},
		{"10.0.0.3", "10.0.0.3:27017", true},
		{"10.0.0.3", "10.0.0.3:37017", false},
		{"10.0.0.3:37017", "10.0.0.4:37017", false},
		{"[fd00::1]", "[fd00::1]:27017", true},
		{"[fd00::1]", "[fd00::1]:37017", false},
		{"[fd00::1", "[fd00::1]:27017", false},
		{"::1", "localhost:27017", false},
		{"::1", "localhost:37017", false},
		{"0:0:0:0:0:0:0:1", "[::1]:27017", true},
		{"fd00::1", "[fd00::1]:27017", true},
	} {
		c.Logf("test %d: %q %q", i, test.a, test.b)
		c.Check(sameAddress(test.a, test.b), gc.Equals, test.want)
		c.Check(sameAddress(test.b, test.a), gc.Equals, test.want)
	}
}

func (s *changesSuite) TestQuorumCheckFailureClassification(c *gc.C) {
	quorumMessage := "Quorum check failed because not enough voting nodes responded"
	for i, test := range []struct {
		err  error
		want bool
	}{{
		err:  nil,
		want: false,
	}, {
		err:  &mgo.QueryError{Code: 11602},
		want: true,
	}, {
		err:  &mgo.QueryError{Code: 74, Message: quorumMessage},
		want: true,
	}, {
		err:  &mgo.QueryError{Code: 74, Message: "No host described in new configuration maps to this node"},
		want: false,
	}, {
		err:  &mgo.QueryError{Code: 13, Message: "not authorized"},
		want: false,
	}, {
		// The message alone outside a QueryError is not a quorum failure.
		err:  errors.New(quorumMessage),
		want: false,
	}, {
		err:  io.EOF,
		want: false,
	}, {
		// A typed nil QueryError must not be dereferenced.
		err:  (*mgo.QueryError)(nil),
		want: false,
	}} {
		c.Logf("test %d: %v", i, test.err)
		c.Check(isQuorumCheckFailure(test.err), gc.Equals, test.want)
	}
}

func (s *changesSuite) TestUnrelatedReconfigErrorDoesNotRepair(c *gc.C) {
	// Fail the test if the repair path is entered at all.
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		c.Errorf("unexpected status call for an unrelated reconfig error")
		return nil, errors.New("unexpected")
	})

	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      666,
		Address: "10.0.0.2",
	}}
	m := &mockSession{
		reconfigErr: &mgo.QueryError{Code: 13, Message: "not authorized"},
	}
	wantMembers := []Member{{
		Id:      666,
		Address: "10.0.0.2",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, gc.ErrorMatches, `cannot remove member 1 from replicaset: not authorized`)
	c.Check(errors.Is(err, repairNeeded), jc.IsFalse)
	c.Check(m.repaired, jc.IsFalse)
	c.Check(m.cfg, gc.IsNil)
}

func (s *changesSuite) TestRepairReportsDownVoterWhenTargetRecovered(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}, {
		Id:      666,
		Address: "10.0.0.3",
	}}
	statusCalls := 0
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		statusCalls++
		return &Status{
			Members: []MemberStatus{{
				Id:      666,
				Healthy: true,
				State:   PrimaryState,
			}, {
				// The target is healthy, so there is nothing to evict.
				Id:      1,
				Healthy: true,
				State:   SecondaryState,
			}, {
				// An untargeted voter stays down, which is the real problem.
				Id:      2,
				Healthy: false,
				State:   DownState,
			}},
		}, nil
	})

	m := &mockSession{}
	err := repairReplicaSet(m, []int{1})
	c.Assert(err, gc.ErrorMatches,
		`cannot repair replicaset: voters \[2\] are unhealthy but not in the removal targets \[1\]`)
	c.Check(statusCalls, gc.Equals, confirmDeadSamples)
	c.Check(m.repaired, jc.IsFalse)
}

func (s *changesSuite) TestRepairForcesNothingWhenTargetIsNotDown(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      666,
		Address: "10.0.0.2",
	}}
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		return &Status{
			Members: []MemberStatus{{
				Id:      666,
				Healthy: true,
				State:   PrimaryState,
			}, {
				// Unhealthy, but neither DOWN nor UNKNOWN, so not evictable.
				Id:      1,
				Healthy: false,
				State:   RecoveringState,
			}},
		}, nil
	})

	m := &mockSession{}
	err := repairReplicaSet(m, []int{1})
	c.Assert(err, jc.ErrorIsNil)
	c.Check(m.repaired, jc.IsFalse)
	c.Check(m.cfg, gc.IsNil)
}

func (s *changesSuite) TestRepairIgnoresBrieflyUnhealthyVoterWhenTargetRecovered(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}, {
		Id:      666,
		Address: "10.0.0.3",
	}}
	statusCalls := 0
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		statusCalls++
		// The untargeted voter is unhealthy in the first sample only.
		other := MemberStatus{
			Id:      2,
			Healthy: statusCalls > 1,
			State:   SecondaryState,
		}
		if statusCalls == 1 {
			other.State = DownState
		}
		return &Status{
			Members: []MemberStatus{{
				Id:      666,
				Healthy: true,
				State:   PrimaryState,
			}, {
				Id:      1,
				Healthy: true,
				State:   SecondaryState,
			}, other},
		}, nil
	})

	m := &mockSession{}
	err := repairReplicaSet(m, []int{1})
	c.Assert(err, jc.ErrorIsNil)
	// The second sample clears the voter, so sampling stops there, and one
	// poll finds the primary.
	c.Check(statusCalls, gc.Equals, 3)
	c.Check(m.repaired, jc.IsFalse)
}

func (s *changesSuite) TestRepairIgnoresStatusMemberNotInConfig(c *gc.C) {
	// Member 1 has already left the config, but a stale status still lists
	// it as down. It must not be treated as an eviction target.
	s.current = []Member{{
		Id:      666,
		Address: "10.0.0.2",
	}, {
		Id:      667,
		Address: "10.0.0.3",
	}}
	s.setupRepairStatus(c)

	m := &mockSession{}
	err := repairReplicaSet(m, []int{1})
	c.Assert(err, jc.ErrorIsNil)
	c.Check(m.repaired, jc.IsFalse)
	c.Check(m.cfg, gc.IsNil)
}

func (s *changesSuite) TestRepairErrorWithNoRemovalTargets(c *gc.C) {
	err := repairReplicaSet(&mockSession{}, nil)
	c.Assert(err, gc.ErrorMatches, `cannot repair replicaset: no removal targets`)
}

func (s *changesSuite) TestAddErrorsWithoutForceWhenQuorumIsLost(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}}
	// Every reconfig loses quorum. Add removes nothing, so there is no
	// member it may force out.
	m := &mockSession{
		failWhen: func(Config) bool { return true },
	}
	err := Add(m, Member{Address: "10.0.0.3"})
	c.Assert(err, gc.ErrorMatches, `repairing replicaset after cannot add member .*"10\.0\.0\.3".* to replicaset: repair needed: Quorum check failed .*: cannot repair replicaset: no removal targets`)
	c.Check(m.repaired, jc.IsFalse)
	c.Check(m.cfg, gc.IsNil)
}

func (s *changesSuite) TestRepairErrorGettingStatus(c *gc.C) {
	failure := errors.New("status failed")
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		return nil, failure
	})

	err := repairReplicaSet(&mockSession{}, []int{1})
	c.Check(errors.Cause(err), gc.Equals, failure)
	c.Check(err, gc.ErrorMatches, `getting rs status to assess repair safety: status failed`)
}

func (s *changesSuite) TestRepairRefusedForUnhealthyVoterOutsideTargets(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}, {
		Id:      666,
		Address: "10.0.0.3",
	}}
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		return &Status{
			Members: []MemberStatus{{
				Id:      666,
				Healthy: true,
				State:   PrimaryState,
			}, {
				Id:      1,
				Healthy: false,
				State:   DownState,
			}, {
				// Not one we were asked to remove, so the repair is ambiguous.
				Id:      2,
				Healthy: false,
				State:   DownState,
			}},
		}, nil
	})

	err := repairReplicaSet(&mockSession{}, []int{1})
	c.Assert(err, gc.ErrorMatches,
		`cannot repair replicaset: voters \[2\] are unhealthy but not in the removal targets \[1\]`)
}

func (s *changesSuite) TestRepairRefusedWithoutPrimaryEligibleMember(c *gc.C) {
	// Make the live voters an arbiter and a priority-zero data-bearing member.
	arbiter := true
	zeroPriority := 0.0
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:       2,
		Address:  "10.0.0.2",
		Arbiter:  &arbiter,
		Priority: &zeroPriority,
	}, {
		Id:       666,
		Address:  "10.0.0.3",
		Priority: &zeroPriority,
	}}

	// Report both non-electable voters as healthy and the target as dead.
	statusCalls := 0
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		statusCalls++
		return &Status{
			Members: []MemberStatus{{
				Id:      2,
				Healthy: true,
				State:   ArbiterState,
			}, {
				Id:      666,
				Healthy: true,
				State:   SecondaryState,
			}, {
				Id:      1,
				Healthy: false,
				State:   DownState,
			}},
		}, nil
	})

	// Refuse the force because no live voter can become primary after eviction.
	m := &mockSession{}
	err := repairReplicaSet(m, []int{1})
	c.Assert(err, gc.ErrorMatches,
		`cannot repair replicaset: evicting \[1\] leaves no live primary-eligible member among voters \[2 666\]`)
	// The decision must use all confirmation samples without forcing a
	// reconfig.
	c.Assert(statusCalls, gc.Equals, confirmDeadSamples)
	c.Assert(m.repaired, jc.IsFalse)
}

func (s *changesSuite) TestRepairErrorsWhenNoPrimaryIsElected(c *gc.C) {
	// Leave one normal data-bearing voter after the dead target is evicted.
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      666,
		Address: "10.0.0.2",
	}}

	// Keep the survivor healthy but secondary through assessment and polling.
	statusCalls := 0
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		statusCalls++
		return &Status{
			Members: []MemberStatus{{
				Id:      666,
				Healthy: true,
				State:   SecondaryState,
			}, {
				Id:      1,
				Healthy: false,
				State:   DownState,
			}},
		}, nil
	})

	// A successful force must not be reported as a successful repair
	// without a primary.
	m := &mockSession{}
	err := repairReplicaSet(m, []int{1})
	c.Assert(err, gc.ErrorMatches, fmt.Sprintf(
		`cannot repair replicaset: no primary elected after %d attempts`, primaryPollAttempts))
	// Every assessment sample and primary poll must have completed.
	c.Assert(statusCalls, gc.Equals, confirmDeadSamples+primaryPollAttempts)
	c.Assert(m.repaired, jc.IsTrue)
}

// unpatchedPrimaryPollInterval keeps the production interval, since
// SetUpTest patches primaryPollInterval to zero.
var unpatchedPrimaryPollInterval = primaryPollInterval

func (s *changesSuite) TestPrimaryPollWaitsLongerThanElectionTimeout(c *gc.C) {
	// Mongo's default election timeout is 10s; keep a margin above it.
	var total time.Duration
	backoff := unpatchedPrimaryPollInterval
	for i := 0; i+1 < primaryPollAttempts; i++ {
		total += backoff
		backoff = time.Duration(float64(backoff) * 1.2)
	}
	c.Check(total >= 15*time.Second, jc.IsTrue, gc.Commentf("total wait %v", total))
}

func (s *changesSuite) TestRepairEvictsPersistentlyDownAndUnknownTargets(c *gc.C) {
	// Both unavailable members are explicit removal targets, so evicting them
	// leaves the consistently healthy member able to form a majority by itself.
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}, {
		Id:      666,
		Address: "10.0.0.3",
	}}
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		return &Status{
			Members: []MemberStatus{{
				Id:      666,
				Healthy: true,
				State:   PrimaryState,
			}, {
				Id:      1,
				Healthy: false,
				State:   DownState,
			}, {
				Id:      2,
				Healthy: false,
				State:   UnknownState,
			}},
		}, nil
	})

	m := &mockSession{}
	err := repairReplicaSet(m, []int{1, 2})
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.repaired, jc.IsTrue)
	c.Assert(m.cfg.Members, jc.DeepEquals, []Member{{
		Id:      666,
		Address: "10.0.0.3",
	}})
}

func (s *changesSuite) TestRepairRefusedForFlappingVoter(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}, {
		Id:      666,
		Address: "10.0.0.3",
	}}
	statusCalls := 0
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		statusCalls++
		flapping := MemberStatus{
			Id:      2,
			Healthy: true,
			State:   SecondaryState,
		}
		if statusCalls == 2 {
			flapping.Healthy = false
			flapping.State = RecoveringState
		}
		return &Status{
			Members: []MemberStatus{{
				Id:      666,
				Healthy: true,
				State:   PrimaryState,
			}, {
				Id:      1,
				Healthy: false,
				State:   DownState,
			}, flapping},
		}, nil
	})

	m := &mockSession{}
	err := repairReplicaSet(m, []int{1})
	c.Assert(err, gc.ErrorMatches,
		`cannot repair replicaset: evicting \[1\] leaves live voters \[666\] of \[2 666\], but a majority needs 2`)
	c.Assert(m.repaired, jc.IsFalse)
	c.Assert(statusCalls, gc.Equals, 3)
}

// transientQuorumStatus reports member 1 as down in the first sample only, as
// if it briefly dropped out and then recovered.
func (s *changesSuite) transientQuorumStatus(c *gc.C) *int {
	statusCalls := 0
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		member := MemberStatus{
			Id:      1,
			Healthy: false,
			State:   DownState,
		}
		statusCalls++
		if statusCalls > 1 {
			// The member has recovered by the second sample.
			member.Healthy = true
			member.State = SecondaryState
		}
		return &Status{
			Name: "test",
			Members: []MemberStatus{{
				Id:      666,
				Healthy: true,
				State:   PrimaryState,
			}, member},
		}, nil
	})

	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      666,
		Address: "10.0.0.2",
	}}
	return &statusCalls
}

func (s *changesSuite) TestRetryWithoutForceForTransientlyDownMember(c *gc.C) {
	statusCalls := s.transientQuorumStatus(c)

	// Lose quorum on the first remove only, as a transient failure would.
	reconfigs := 0
	m := &mockSession{
		failWhen: func(cfg Config) bool {
			reconfigs++
			return reconfigs == 1
		},
	}
	wantMembers := []Member{{
		Id:      666,
		Address: "10.0.0.2",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(m.repaired, jc.IsFalse)
	c.Check(reconfigs, gc.Equals, 2)
	// The target recovers in the second sample, so sampling stops there, and
	// one poll finds the primary.
	c.Check(*statusCalls, gc.Equals, 3)
	c.Check(m.cfg, jc.DeepEquals, &Config{
		Version: 1,
		Members: []Member{{
			Id:      666,
			Address: "10.0.0.2",
		}},
	})
}

func (s *changesSuite) TestRetryWithoutForceWaitsForPrimaryAfterStepDown(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      666,
		Address: "10.0.0.2",
	}}
	// The primary stepped down, so every member is a healthy secondary until
	// a new primary is elected.
	statusCalls := 0
	// One assessment sample, then a primary appears on the third poll.
	electedAt := 1 + 3
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		statusCalls++
		survivor := MemberStatus{
			Id:      666,
			Healthy: true,
			State:   SecondaryState,
		}
		if statusCalls >= electedAt {
			survivor.State = PrimaryState
		}
		return &Status{
			Members: []MemberStatus{survivor, {
				Id:      1,
				Healthy: true,
				State:   SecondaryState,
			}},
		}, nil
	})

	reconfigs := 0
	m := &mockSession{
		failWhen: func(cfg Config) bool {
			reconfigs++
			// The retry must only happen once a primary is visible.
			if reconfigs > 1 {
				c.Check(statusCalls, gc.Equals, electedAt)
			}
			return reconfigs == 1
		},
	}
	wantMembers := []Member{{
		Id:      666,
		Address: "10.0.0.2",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, jc.ErrorIsNil)
	c.Check(m.repaired, jc.IsFalse)
	c.Check(reconfigs, gc.Equals, 2)
	c.Check(statusCalls, gc.Equals, electedAt)
	c.Check(m.cfg, jc.DeepEquals, &Config{
		Version: 1,
		Members: []Member{{
			Id:      666,
			Address: "10.0.0.2",
		}},
	})
}

func (s *changesSuite) TestRetryWithoutForceStillLosesQuorum(c *gc.C) {
	statusCalls := s.transientQuorumStatus(c)

	// Lose quorum on every remove, so the normal retry fails too.
	reconfigs := 0
	m := &mockSession{
		failWhen: func(cfg Config) bool {
			reconfigs++
			return true
		},
	}
	wantMembers := []Member{{
		Id:      666,
		Address: "10.0.0.2",
	}}
	err := Set(m, wantMembers)
	c.Assert(err, gc.ErrorMatches,
		`cannot apply Set changes after replicaset repair attempt: cannot remove member 1 from replicaset: repair needed: Quorum check failed because not enough voting nodes responded`)
	c.Check(errors.Is(err, repairNeeded), jc.IsFalse)
	c.Check(m.cfg, gc.IsNil)
	c.Check(m.repaired, jc.IsFalse)
	// One normal retry, and no second repair attempt.
	c.Check(reconfigs, gc.Equals, 2)
	c.Check(*statusCalls, gc.Equals, 3)
}

func (s *changesSuite) TestRepairEvictsOnlyPersistentlyUnavailableTargets(c *gc.C) {
	s.current = []Member{{
		Id:      1,
		Address: "10.0.0.1",
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}, {
		Id:      666,
		Address: "10.0.0.3",
	}, {
		Id:      667,
		Address: "10.0.0.4",
	}}
	statusCalls := 0
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		statusCalls++
		transient := MemberStatus{
			Id:      2,
			Healthy: false,
			State:   DownState,
		}
		if statusCalls > 1 {
			transient.Healthy = true
			transient.State = SecondaryState
		}
		return &Status{
			Members: []MemberStatus{{
				Id:      666,
				Healthy: true,
				State:   PrimaryState,
			}, {
				Id:      667,
				Healthy: true,
				State:   SecondaryState,
			}, {
				Id:      1,
				Healthy: false,
				State:   DownState,
			}, transient},
		}, nil
	})

	m := &mockSession{}
	err := repairReplicaSet(m, []int{1, 2})
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.repaired, jc.IsTrue)
	c.Assert(m.cfg.Members, gc.HasLen, 3)
	gotIds := make([]int, len(m.cfg.Members))
	for i, member := range m.cfg.Members {
		gotIds[i] = member.Id
	}
	c.Check(gotIds, gc.DeepEquals, []int{2, 666, 667})
}

// patchConfigFromSession makes getCurrentConfig and getSessionConfig reflect
// the last reconfig applied to the mock session, so a post-repair retry sees
// the repaired config rather than the initial one.
func (s *changesSuite) patchConfigFromSession(c *gc.C, m *mockSession) {
	currentConfig := func(session mgoSession) (*Config, error) {
		if m.cfg == nil {
			return &Config{
				Members: append([]Member(nil), s.current...),
			}, nil
		}
		cfg := *m.cfg
		cfg.Members = append([]Member(nil), m.cfg.Members...)
		return &cfg, nil
	}
	s.PatchValue(&getCurrentConfig, currentConfig)
	s.PatchValue(&getSessionConfig, currentConfig)
}

func (s *changesSuite) setupRepairStatus(c *gc.C) {
	// replSetGetStatus reports every member of the config, so a healthy member
	// that some tests add (667) is listed here too. Tests whose config does not
	// contain it are unaffected, because only config members are considered.
	mockCurrentStatus := func(session mgoSession) (*Status, error) {
		return &Status{
			Name: "test",
			Members: []MemberStatus{{
				Id:      666,
				Healthy: true,
				State:   PrimaryState,
			}, {
				Id:      667,
				Healthy: true,
				State:   SecondaryState,
			}, {
				Id:      1,
				Healthy: false,
				State:   DownState,
			}},
		}, nil
	}
	s.PatchValue(&getCurrentStatus, mockCurrentStatus)
}

type fmtConfigForLogSuite struct {
	testing.IsolationSuite
}

var _ = gc.Suite(&fmtConfigForLogSuite{})

func (s *fmtConfigForLogSuite) TestSimpleFormatting(c *gc.C) {
	anInt := func(v int) *int {
		return &v
	}
	cfg := &Config{
		Name:    "juju",
		Version: 1,
		Term:    1,
		Members: []Member{{
			Id:      2,
			Address: "192.168.0.10:37017",
			Tags:    map[string]string{"juju-machine-id": "1"},
			Votes:   anInt(1),
		}, {
			Id:      1,
			Address: "192.168.0.9:37017",
			Tags:    map[string]string{"juju-machine-id": "0"},
			Votes:   nil,
		}, {
			Id:      3,
			Address: "192.168.0.27:37017",
			Tags:    map[string]string{"juju-machine-id": "2"},
			Votes:   anInt(0),
		}},
	}
	c.Check(fmtConfigForLog(cfg), gc.Equals, `{
  Name: juju,
  Version: 1,
  Term: 1,
  Protocol Version: 0,
  Members: {
    {1 "192.168.0.9:37017" juju-machine-id:0 voting},
    {2 "192.168.0.10:37017" juju-machine-id:1 voting},
    {3 "192.168.0.27:37017" juju-machine-id:2 not-voting},
  },
}`)
	// no side effect, the config is not sorted:
	c.Check(cfg.Members[0].Id, gc.Equals, 2)
	c.Check(cfg.Members[1].Id, gc.Equals, 1)
	c.Check(cfg.Members[2].Id, gc.Equals, 3)
	cfg2 := &Config{
		Name:            "juju",
		Version:         2,
		Term:            1,
		ProtocolVersion: 1,
		Members:         append([]Member(nil), cfg.Members...),
	}
	cfg2.Members[1].Votes = anInt(0)
	cfg2.Members[2].Votes = anInt(1)
	c.Check(fmtConfigForLog(cfg2), gc.Equals, `{
  Name: juju,
  Version: 2,
  Term: 1,
  Protocol Version: 1,
  Members: {
    {1 "192.168.0.9:37017" juju-machine-id:0 not-voting},
    {2 "192.168.0.10:37017" juju-machine-id:1 voting},
    {3 "192.168.0.27:37017" juju-machine-id:2 voting},
  },
}`)
}

func (s *changesSuite) TestRepairEvictsDeadNonVoter(c *gc.C) {
	// A dead non-voting member can be evicted without affecting the majority,
	// so the repair proceeds even though only two voters remain.
	noVotes := 0
	zeroPriority := 0.0
	s.current = []Member{{
		Id:       1,
		Address:  "10.0.0.1",
		Priority: &zeroPriority,
		Votes:    &noVotes,
	}, {
		Id:      2,
		Address: "10.0.0.2",
	}, {
		Id:      666,
		Address: "10.0.0.3",
	}}
	s.PatchValue(&getCurrentStatus, func(session mgoSession) (*Status, error) {
		return &Status{
			Members: []MemberStatus{{
				Id:      666,
				Healthy: true,
				State:   PrimaryState,
			}, {
				Id:      2,
				Healthy: true,
				State:   SecondaryState,
			}, {
				Id:      1,
				Healthy: false,
				State:   DownState,
			}},
		}, nil
	})

	m := &mockSession{}
	err := repairReplicaSet(m, []int{1})
	c.Assert(err, jc.ErrorIsNil)
	c.Assert(m.repaired, jc.IsTrue)
	c.Assert(m.cfg.Members, gc.HasLen, 2)
	for _, member := range m.cfg.Members {
		c.Check(member.Id, gc.Not(gc.Equals), 1)
	}
}
