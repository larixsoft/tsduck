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
#include "tsReactorHandlerInterface.h"
#include "tsReport.h"
#include "tsNames.h"

// Depending on the operating system, we use distinct forms of kernel event queues.
//! @cond nodoxygen
#if defined(TS_LINUX)
    // Use epoll(), a Linux specific feature.
    // Future: investigate io_uring.
    #define TS_USE_EPOLL 1
    #include "tsBeforeStandardHeaders.h"
    #include <sys/epoll.h>
    #include <sys/eventfd.h>
    #include <sys/timerfd.h>
    #include "tsAfterStandardHeaders.h"
#elif defined(TS_MAC) || defined(TS_BSD)
    // Use kqueue(), as found on macOS and all BSD systems.
    #define TS_USE_KQUEUE 1
    #include "tsBeforeStandardHeaders.h"
    #include <sys/event.h>
    #include "tsAfterStandardHeaders.h"
#elif defined(TS_WINDOWS)
    // Use I/O completion ports, a Windows feature.
    #define TS_USE_IOCP 1
#else
    #error "Reactor is not supported on this operating system"
#endif
//! @endcond

namespace ts {
    //!
    //! Event-driven reactor class implementing the "event loop" pattern.
    //! @ingroup libtscore reactor
    //!
    //! To be completed...
    //!
    //! Synchronisation
    //! ---------------
    //! The Reactor shall be used from one single thread. Unless specified otherwise, all methods
    //! shall be invoked from the thread of the reactor, where the event loop is run.
    //! All handlers are invoked in the context of the thread which invoked processEventLoop(),
    //! except worker handlers which are invoked in the context of a worker thread. Therefore,
    //! worker handlers are not allowed to called methods of the reactor.
    //!
    class TSCOREDLL Reactor
    {
        TS_NOBUILD_NOCOPY(Reactor);
    private:
        class Resource;

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
        //! Open and initialize the Reactor.
        //! Must be invoked before running the event loop or registering events.
        //! @return True on success, false on error.
        //!
        bool open();

        //!
        //! Close the Reactor.
        //! Must not be called from within the event loop.
        //! Automatically done in the destructor.
        //! @return True on success, false on error.
        //!
        bool close();

        //!
        //! Check if the Reactor is open.
        //! @return True if the Reactor is open, false otherwise.
        //!
        bool isOpen() const { return _is_open; }

        //!
        //! Process events until exit is requested.
        //! @return The status from exitEventLoop(). If exitEventLoop() is invoked several
        //! times, the returned value is false if exitEventLoop() has been called at least
        //! once with @a success being false.
        //!
        bool processEventLoop();

        //!
        //! Exit processEventLoop() as soon as possible.
        //! This method is typically invoked from a handler.
        //! @param [in] success The value that processEventLoop() shall return.
        //!
        void exitEventLoop(bool success = true);

        //--------------------------------------------------------------------
        // TIMERS
        //--------------------------------------------------------------------

        //!
        //! Add a timer in the reactor.
        //!
        //! If the timer is one-shot (the default), the timer is automatically deleted after
        //! its expiration, after the handler is invoked. If the timer is repeated, it shall
        //! be explicitly canceled if needed.
        //!
        //! @param [in] handler Address of a handler to call when the timer expires. Return an error if set as @c nullptr.
        //! @param [in] duration Duration of the timer. Must be strictly positive.
        //! @param [in] repeat If true, the timer is repeated at regular intervals, every @a duration, until canceled.
        //! @return The identity of the timer. Invalid in case of error.
        //!
        template <class Rep, class Period>
        EventId newTimer(ReactorTimerHandlerInterface* handler, cn::duration<Rep, Period> duration, bool repeat = false)
        {
            return newTimerImpl(handler, cn::duration_cast<cn::milliseconds>(duration), repeat);
        }

        //!
        //! Cancel a timer.
        //! @param [in] id Timer to cancel.
        //! @param [in] display_errors If true, display errors. False by default. Canceling a non-repeated timer
        //! may lead to race conditions where a timer is canceled just after it expired and is no longer valid.
        //! @return True on success, false on error.
        //!
        bool cancelTimer(EventId id, bool display_errors = false);

        //--------------------------------------------------------------------
        // USER EVENTS
        //--------------------------------------------------------------------

        //!
        //! Add a user event in the reactor.
        //! @param [in] handler Address of a handler to call when the event is signalled. Return an error if set as @c nullptr.
        //! @return The identity of the user event. Invalid in case of error.
        //!
        EventId newEvent(ReactorEventHandlerInterface* handler);

        //!
        //! Signal a user event in the reactor.
        //! As an exception to the single-thread-reactor rule, this method can be invoked from any thread.
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
        // Types of source events.
        enum class EventType {NONE, TIMER, EVENT};
        static const Names& EventTypeNames();

        // Description of one resource to wait for (I/O, timer, event, etc.)
        // All implementations (epoll, kqueue, iocp) associate some "user pointer" to an event. This pointer is the address
        // of a Resource object. We must be sure that the associated Resource object is stable (same address, still valid)
        // at completion of events. To track potential errors in our interpretation of the various operating system logics,
        // we use some internal consistency checks.
        class Resource
        {
            TS_DEFAULT_COPY_MOVE(Resource);
        public:
            // Values for canary (internal consistency check).
            static constexpr uint32_t GOOD = 0x474F4F44; // "GOOD"
            static constexpr uint32_t BAD  = 0x42414453; // "BADS"

            volatile uint32_t        canary;
            EventType                type = EventType::NONE;
            ReactorHandlerInterface* handler = nullptr;
            bool                     repeat = false;  // Repeatable timer.

#if defined(TS_USE_EPOLL) || defined(TS_USE_KQUEUE)
            int      fd = -1;                         // General-purpose file descriptor.
#elif defined(TS_USE_IOCP)
            Reactor* reactor = nullptr;               // Link to parent reactor, when called in APC.
            ::HANDLE handle = INVALID_HANDLE_VALUE;   // General-purpose handle.
#endif

            // Constructors and destructor.
            Resource();
            ~Resource();
        };

        // Map of active resources by id. The Resource blocks are automatically released.
        using ResourceMap = std::map<EventId, std::unique_ptr<Resource>>;

        // Reactor private members.
        Report& _report;
        bool    _is_open = false;
        bool    _exit_requested = false;    // Exit event loop when possible.
        bool    _exit_success = true;       // Exit status for event loop.
        std::set<Resource*> _resources {};  // Existing allocated resources.

        // Implementation of public template methods.
        EventId newTimerImpl(ReactorTimerHandlerInterface* handler, cn::milliseconds duration, bool repeat);

        // Error checking.
        bool checkOpen();

        // Allocate and register a new resource.
        Resource* newResource(EventType type, ReactorHandlerInterface* handler);

        // Deregister and deallocate a resource.
        void deleteResource(Resource* res);

        // Check that a resource pointer is valid. Detect incorrect understanding of system mechanisms.
        // If expected is not NONE, check the event type and return the actual event type.
        // Return a static error message or nullptr if the resource pointer seems valid.
        static const UChar* ResourceErrorMessage(Resource* res, EventType expected, EventType& actual);

        // Check that a resource pointer is valid, registered, and display an error message if required.
        // is true, do not use the Reactor object (when used from another thread or asynchronous system call).
        bool validateResource(Resource* res, EventType type, bool display_errors);

        // Close the system part of a resource.
        bool sysDeleteTimer(Resource* res, bool display_errors);
        bool sysDeleteEvent(Resource* res, bool display_errors);

        // All implementations are based on some form of kernel queue. The core of the event loop is a wait system call
        // which uses an array of system structures to receive the completed events. The vector of these system structures
        // must be at least as large as the number of pending events, which is potentially different at each wait call.
        // We try to avoid too many reallocations by keeping some minimal number of events.
        static constexpr size_t MIN_WAIT_EVENTS = 16;

        // Adjust the size of the system structures describing events to wait for. See MIN_WAIT_EVENTS.
        template <class SYSEVENT>
        void adjustEventVector(std::vector<SYSEVENT>& events)
        {
            if (events.size() < _resources.size() || events.size() > _resources.size() + 10 * MIN_WAIT_EVENTS) {
                // Current vector is too small or significantly too large.
                events.resize(_resources.size() + MIN_WAIT_EVENTS);
            }
        }

#if defined(TS_USE_EPOLL)

        // Aliases for types in timespec structure (not the same types on all platforms).
        using timespec_sec_t  = decltype(::timespec::tv_sec);
        using timespec_nsec_t = decltype(::timespec::tv_nsec);

        int _epoll = -1;  // File descriptor for epoll().

#elif defined(TS_USE_KQUEUE)

        // Aliases for types in kevent structure (not the same types on all platforms).
        using kevent_t       = struct kevent;
        using kevent_ident_t = decltype(kevent_t::ident);
        using kevent_data_t  = decltype(kevent_t::data);

        int _kqueue = -1;  // File descriptor for kqueue(). Can be used in another thread (signal event).

#elif defined(TS_USE_IOCP)

        ::HANDLE _iocp = INVALID_HANDLE_VALUE;  // Handle for the I/O completion port.

        // Asynchronous timer completion routine.
        static void APIENTRY WinTimerCompletion(::LPVOID arg, ::DWORD timer_low, ::DWORD time_high);

#endif
    };
}
