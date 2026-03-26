//----------------------------------------------------------------------------
//
// TSDuck - The MPEG Transport Stream Toolkit
// Copyright (c) 2005-2026, Thierry Lelegard
// BSD-2-Clause license, see LICENSE.txt file or https://tsduck.io/license
//
//----------------------------------------------------------------------------

#include "tsReactor.h"


//----------------------------------------------------------------------------
// Constructors and destructors.
//----------------------------------------------------------------------------

ts::Reactor::Reactor(Report& report) :
    _report(report)
{
}

ts::Reactor::~Reactor()
{
}

ts::Reactor::HandlerInterface::~HandlerInterface()
{
}


//----------------------------------------------------------------------------
// Exit processEventLoop() as soon as possible.
//----------------------------------------------------------------------------

void ts::Reactor::exitEventLoop(bool success)
{
    _exit_requested = true;
    if (!success) {
        _exit_success = false;
    }
}


//----------------------------------------------------------------------------
// Process events until exit is requested.
//----------------------------------------------------------------------------

bool ts::Reactor::processEventLoop()
{
    _exit_requested = false;
    _exit_success = true;

    //@@@

    return _exit_success;
}


//----------------------------------------------------------------------------
// Add a timer.
//----------------------------------------------------------------------------

ts::Reactor::TimerId ts::Reactor::newTimerImpl(HandlerInterface* handler, TimerHandler method, cn::milliseconds duration, bool repeat)
{
    // Check parameters.
    if (handler == nullptr || method == nullptr || duration <= cn::milliseconds::zero()) {
        _report.error(u"internal error: invalid parameter in Reactor::newTimer");
        return TimerId();
    }


    return TimerId(); //@@@@
}


//----------------------------------------------------------------------------
// Cancel a timer.
//----------------------------------------------------------------------------

bool ts::Reactor::cancelTimer(TimerId id, bool display_errors)
{
    // Check parameters.
    if (!id.isValid()) {
        if (display_errors) {
            _report.error(u"internal error: invalid id in Reactor::cancelTimer");
        }
        return false;
    }



    return false;  //@@@@@
}


//----------------------------------------------------------------------------
// Add a user event in the reactor.
//----------------------------------------------------------------------------

ts::Reactor::EventId ts::Reactor::newEventImpl(HandlerInterface* handler, EventHandler method)
{
    // Check parameters.
    if (handler == nullptr || method == nullptr) {
        _report.error(u"internal error: invalid parameter in Reactor::newEvent");
        return EventId();
    }


    return EventId();  //@@@@
}


//----------------------------------------------------------------------------
// Signal a user event in the reactor.
//----------------------------------------------------------------------------

bool ts::Reactor::signalEvent(EventId id)
{
    // Check parameters.
    if (!id.isValid()) {
        _report.error(u"internal error: invalid id in Reactor::signalEvent");
        return false;
    }


    return false;  //@@@@@
}


//----------------------------------------------------------------------------
// Delete a user event.
//----------------------------------------------------------------------------

bool ts::Reactor::deleteEvent(EventId id)
{
    // Check parameters. Fail without error message.
    if (!id.isValid()) {
        _report.error(u"internal error: invalid id in Reactor::deleteEvent");
        return false;
    }


    return false;  //@@@@@
}
