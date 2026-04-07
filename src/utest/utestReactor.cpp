//----------------------------------------------------------------------------
//
// TSDuck - The MPEG Transport Stream Toolkit
// Copyright (c) 2005-2026, Thierry Lelegard
// BSD-2-Clause license, see LICENSE.txt file or https://tsduck.io/license
//
//----------------------------------------------------------------------------
//
//  TSUnit test suite for subclasses of ts::Reactor
//
//----------------------------------------------------------------------------

#include "tsReactor.h"
#include "tsTime.h"
#include "tsCerrReport.h"
#include "tsSysUtils.h"
#include "tsunit.h"


//----------------------------------------------------------------------------
// The test fixture
//----------------------------------------------------------------------------

class ReactorTest: public tsunit::Test
{
    TSUNIT_DECLARE_TEST(Timer);

public:
    virtual void beforeTestSuite() override;

private:
    cn::milliseconds _precision {};
};

TSUNIT_REGISTER(ReactorTest);


//----------------------------------------------------------------------------
// Initialization.
//----------------------------------------------------------------------------

// Test suite initialization method.
void ReactorTest::beforeTestSuite()
{
    _precision = cn::milliseconds(2);
    ts::SetTimersPrecision(_precision);
    debug() << "ReactorTest: timer precision = " << ts::UString::Chrono(_precision) << std::endl;
}


//----------------------------------------------------------------------------
// Unitary tests.
//----------------------------------------------------------------------------

namespace {
    class HandlerTimer: public ts::ReactorTimerHandlerInterface, public ts::ReactorEventHandlerInterface
    {
    private:
        std::ostream&    _debug;
        cn::milliseconds _precision;

    public:
        HandlerTimer() = delete;
        HandlerTimer(std::ostream& debug, cn::milliseconds precision) :
            _debug(debug),
            _precision(precision)
        {}

        ts::Time start {};

        ts::EventId timer1 {};
        ts::EventId timer2 {};
        ts::EventId event1 {};

        size_t timer1_count = 0;
        size_t timer2_count = 0;
        size_t event1_count = 0;

        virtual void handleTimer(ts::Reactor& reactor, ts::EventId id) override;
        virtual void handleUserEvent(ts::Reactor& reactor, ts::EventId id) override;
    };

    void HandlerTimer::handleTimer(ts::Reactor& reactor, ts::EventId id)
    {
        _debug << "HandlerTimer::handleTimer: id: " << id.toString() << ", timer1 count: " << timer1_count << ", timer2 count: " << timer2_count << std::endl;

        if (id == timer1) {
            // Repeated timer: 200 ms
            const ts::Time now(ts::Time::CurrentUTC());
            TSUNIT_ASSERT(id == timer1);
            timer1_count++;
            TSUNIT_ASSERT(timer1_count < 3);
            TSUNIT_ASSUME(timer2_count == 1);
            if (timer1_count == 1) {
                TSUNIT_ASSERT(now >= start + cn::milliseconds(200) - _precision);
            }
            else if (timer1_count == 2) {
                TSUNIT_ASSERT(now >= start + cn::milliseconds(400) - _precision);
                TSUNIT_ASSERT(reactor.cancelTimer(id, true));
                TSUNIT_ASSERT(reactor.signalEvent(event1));
            }
        }
        else if (id == timer2) {
            // One-shot timer: 100 ms
            const ts::Time now(ts::Time::CurrentUTC());
            TSUNIT_ASSERT(id == timer2);
            timer2_count++;
            TSUNIT_EQUAL(1, timer2_count);
            TSUNIT_ASSUME(timer1_count == 0);
            TSUNIT_ASSERT(now >= start + cn::milliseconds(100) - _precision);
        }
        else {
            TSUNIT_ASSERT(false);
        }
    }

    void HandlerTimer::handleUserEvent(ts::Reactor& reactor, ts::EventId id)
    {
        _debug << "HandlerTimer::handleUserEvent: id: " << id.toString() << ", event1 count: " << event1_count << std::endl;

        const ts::Time now(ts::Time::CurrentUTC());
        TSUNIT_ASSERT(id == event1);
        event1_count++;
        TSUNIT_EQUAL(1, event1_count);
        TSUNIT_EQUAL(2, timer1_count);
        TSUNIT_EQUAL(1, timer2_count);
        TSUNIT_ASSERT(now >= start + cn::milliseconds(400) - _precision);
        reactor.exitEventLoop();
    }
}

TSUNIT_DEFINE_TEST(Timer)
{
    ts::Reactor reactor(CERR);
    HandlerTimer test(debug(), _precision);

    TSUNIT_ASSERT(!reactor.isOpen());
    TSUNIT_ASSERT(reactor.open());
    TSUNIT_ASSERT(reactor.isOpen());

    test.timer1 = reactor.newTimer(&test, cn::milliseconds(200), true);
    TSUNIT_ASSERT(test.timer1.isValid());
    debug() << "ReactorTest::Timer: timer1 id: " << test.timer1.toString() << std::endl;

    test.timer2 = reactor.newTimer(&test, cn::milliseconds(100), false);
    TSUNIT_ASSERT(test.timer2.isValid());
    debug() << "ReactorTest::Timer: timer2 id: " << test.timer2.toString() << std::endl;

    test.event1 = reactor.newEvent(&test);
    TSUNIT_ASSERT(test.event1.isValid());
    debug() << "ReactorTest::Timer: event1 id: " << test.event1.toString() << std::endl;

    test.start = ts::Time::CurrentUTC();
    TSUNIT_ASSERT(reactor.processEventLoop());

    const ts::Time now(ts::Time::CurrentUTC());
    TSUNIT_ASSERT(now >= test.start + cn::milliseconds(400) - _precision);

    TSUNIT_EQUAL(1, test.event1_count);
    TSUNIT_EQUAL(2, test.timer1_count);
    TSUNIT_EQUAL(1, test.timer2_count);

    TSUNIT_ASSERT(reactor.isOpen());
    TSUNIT_ASSERT(reactor.close());
    TSUNIT_ASSERT(!reactor.isOpen());
}
