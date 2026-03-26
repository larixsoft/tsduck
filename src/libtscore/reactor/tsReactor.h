//----------------------------------------------------------------------------
//
// TSDuck - The MPEG Transport Stream Toolkit
// Copyright (c) 2005-2026, Thierry Lelegard
// BSD-2-Clause license, see LICENSE.txt file or https://tsduck.io/license
//
//----------------------------------------------------------------------------
//!
//!  @file
//!  Event-driven reactor class implementing the "event loop" pattern.
//!
//----------------------------------------------------------------------------

#pragma once
#include "tsReport.h"

namespace ts {
    //!
    //! Event-driven reactor class implementing the "event loop" pattern.
    //! @ingroup libtscore reactor
    //!
    //! To be completed...
    //!
    //! Synchronisation: The Reactor shall be used from one single thread. Unless specified otherwise,
    //! all methods shall be invoked from the thread of the reactor, where the event loop is run.
    //! All handlers are invoked in the context of the thread which invoked processEventLoop(),
    //! except worker handlers which are invoked in the context of a worker thread. Therefore,
    //! worker handlers are not allowed to called methods of the reactor.
    //!
    class TSCOREDLL Reactor
    {
        TS_NOBUILD_NOCOPY(Reactor);
    public:
        //!
        //! Constructor.
        //! @param [in,out] report Where to report errors.
        //!
        Reactor(Report& report);

        //!
        //! Destructor.
        //! All resources, such as pending timers or user events, are deleted.
        //!
        ~Reactor();

        //!
        //! Access the Report which is associated with the reactor.
        //! Can be called from another thread if the Report object is thread-safe.
        //! @return A reference to the associated report.
        //!
        Report& report() { return _report; }

        //!
        //! Exit processEventLoop() as soon as possible.
        //! This method is typically invoked from a handler.
        //! @param [in] success The value that processEventLoop() shall return.
        //!
        void exitEventLoop(bool success = true);

        //!
        //! Process events until exit is requested.
        //! @return The status from exitEventLoop(). If exitEventLoop() is invoked several
        //! times, the returned value is false if exitEventLoop() has been called at least
        //! once with @a success being false.
        //! 
        bool processEventLoop();

        //!
        //! Interface class for all Reactor handlers.
        //!
        //! All classes which must be used as handlers must inherit from that class.
        //! 
        //! Unlike other types of handlers, there is no specific virtual methods to call when a specific type of event
        //! occurs (timer for instance). For each event, a pointer-to-method shall be passed. Rationale: in an
        //! application-defined handler class, there are possibly several types of timers, events, etc. Having them
        //! go through the same virtual method is not efficient.
        //!
        class TSCOREDLL HandlerInterface
        {
            TS_INTERFACE(HandlerInterface);
        };

        //!
        //! A way to identify sources of events (I/O, timers, etc).
        //! This is a small object which can be passed by value at zero cost.
        //!
        class TSCOREDLL SourceId
        {
        public:
            //!
            //! Default constructor.
            //!
            SourceId() = default;
            //!
            //! Check if the source id is valid.
            //! @return True if the source id is valid.
            //!
            bool isValid() const { return _id != _invalid; }
            //!
            //! Invalidate the content of this object.
            //!
            void invalidate() { _id = _invalid; }
        private:
#if defined(TS_WINDOWS)
            using Id = ::HANDLE;
            static constexpr Id _invalid = INVALID_HANDLE_VALUE;
#else
            using Id = int64_t; // upper 32 bits are a counter, lower 32 bits contains a file descriptor.
            static constexpr Id _invalid = -1;
#endif
            friend class Reactor;
            Id _id = _invalid;
        };

        //--------------------------------------------------------------------
        // TIMERS
        //--------------------------------------------------------------------

        //!
        //! Identify a timer.
        //!
        using TimerId = SourceId;

        //!
        //! Profile of a timer handler in a HandlerInterface.
        //! @param [in] id Id of the timer which expires.
        //!
        using TimerHandler = void (HandlerInterface::*)(TimerId id);

        //!
        //! Add a timer in the reactor.
        //! 
        //! If the timer is one-shot (the default), the timer is automatically deleted after
        //! its expiration, after the handler is invoked. If the timer is repeated, it shall
        //! be explicitly canceled if needed.
        //! 
        //! @param [in] handler Address of a HandlerInterface to call when the timer expires.
        //! @param [in] method Which method to invoke in @a handler when the timer expires.
        //! @param [in] duration Duration of the timer. Must be strictly positive.
        //! @param [in] repeat If true, the timer is repeated at regular intervals, every @a duration, until canceled.
        //! @return The identity of the timer. Invalid in case of error.
        //!
        template <class HANDLER, class Rep, class Period>
            requires std::derived_from<HANDLER, HandlerInterface>
        TimerId newTimer(HANDLER* handler, void (HANDLER::*method)(TimerId id), cn::duration<Rep, Period> duration, bool repeat = false)
        {
            return newTimerImpl(handler, static_cast<TimerHandler>(method), cn::duration_cast<cn::milliseconds>(duration), repeat);
        }

        //!
        //! Cancel a timer.
        //! @param [in] id Timer to cancel.
        //! @param [in] display_errors If true, display errors. False by default. Canceling a non-repeated timer
        //! may lead to race conditions where a timer is canceled just after it expired and is no longer valid.
        //! @return True on success, false on error.
        //! 
        bool cancelTimer(TimerId id, bool display_errors = false);

        //--------------------------------------------------------------------
        // USER EVENTS
        //--------------------------------------------------------------------

        //!
        //! Identify a user event.
        //!
        using EventId = SourceId;

        //!
        //! Profile of a user event handler in a HandlerInterface.
        //! @param [in] id Id of the event which is signaled.
        //!
        using EventHandler = void (HandlerInterface::*)(EventId id);

        //!
        //! Add a user event in the reactor.
        //! @param [in] handler Address of a HandlerInterface to call when the event is signalled.
        //! @param [in] method Which method to invoke in @a handler when the event is signalled.
        //! @return The identity of the user event. Invalid in case of error.
        //!
        template <class HANDLER>
            requires std::derived_from<HANDLER, HandlerInterface>
        EventId newEvent(HANDLER* handler, void (HANDLER::*method)(EventId id))
        {
            return newEventImpl(handler, static_cast<EventHandler>(method));
        }

        //!
        //! Signal a user event in the reactor.
        //! This method can be invoked from any thread.
        //! @param [in] id Event to signal.
        //! @return True on success, false on error.
        //! 
        bool signalEvent(EventId id);

        //!
        //! Delete a user event.
        //! @param [in] id Event to delete.
        //! @return True on success, false on error.
        //!
        bool deleteEvent(EventId id);

    private:
        Report& _report;
        bool    _exit_requested = false;  // Exit event loop when possible.
        bool    _exit_success = true;     // Exit status for event loop.

        // Implementation of template methods.
        TimerId newTimerImpl(HandlerInterface* handler, TimerHandler method, cn::milliseconds duration, bool repeat);
        EventId newEventImpl(HandlerInterface* handler, EventHandler method);
    };
}
