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
#include "tsCerrReport.h"
#include "tsunit.h"


//----------------------------------------------------------------------------
// The test fixture
//----------------------------------------------------------------------------

class ReactorTest: public tsunit::Test
{
    TSUNIT_DECLARE_TEST(Timer);
};

TSUNIT_REGISTER(ReactorTest);


//----------------------------------------------------------------------------
// Unitary tests.
//----------------------------------------------------------------------------

namespace {
    class Test1: public ts::Reactor::HandlerInterface
    {
    public:
        void timeout(ts::Reactor::TimerId dummy) {}
    };
}

TSUNIT_DEFINE_TEST(Timer)
{
    ts::Reactor reactor(CERR);
    Test1 test;
    reactor.newTimer(&test, &Test1::timeout, cn::seconds(2));
}
