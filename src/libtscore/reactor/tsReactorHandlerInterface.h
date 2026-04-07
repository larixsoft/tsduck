//----------------------------------------------------------------------------
//
// TSDuck - The MPEG Transport Stream Toolkit
// Copyright (c) 2005-2026, Thierry Lelegard
// BSD-2-Clause license, see LICENSE.txt file or https://tsduck.io/license
//
//----------------------------------------------------------------------------
//!
//!  @file
//!  All interface classes which are used as event handlers in a Reactor.
//!
//----------------------------------------------------------------------------

#pragma once
#include "tsEventId.h"

namespace ts {

    class Reactor;

    //!
    //! Base interface class for all Reactor handlers.
    //! All classes which must be used as handlers must inherit from that class.
    //!
    class TSCOREDLL ReactorHandlerInterface
    {
        TS_INTERFACE(ReactorHandlerInterface);
    };

    //!
    //! Interface class for timer Reactor handlers.
    //!
    class TSCOREDLL ReactorTimerHandlerInterface : public ReactorHandlerInterface
    {
        TS_SUBINTERFACE(ReactorTimerHandlerInterface);
    public:
        //!
        //! Handle a timer in a Reactor.
        //! @param [in,out] reactor Reactor into which the handler is invoked.
        //! @param [in] id Id of the timer which expires.
        //!
        virtual void handleTimer(Reactor& reactor, EventId id) = 0;
    };

    //!
    //! Interface class for user-defined event Reactor handlers.
    //!
    class TSCOREDLL ReactorEventHandlerInterface : public ReactorHandlerInterface
    {
        TS_SUBINTERFACE(ReactorEventHandlerInterface);
    public:
        //!
        //! Handle a user-defined event in a Reactor.
        //! @param [in,out] reactor Reactor into which the handler is invoked.
        //! @param [in] id Id of the event which was signaled.
        //!
        virtual void handleUserEvent(Reactor& reactor, EventId id) = 0;
    };
}
