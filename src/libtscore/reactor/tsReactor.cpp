//----------------------------------------------------------------------------
//
// TSDuck - The MPEG Transport Stream Toolkit
// Copyright (c) 2005-2026, Thierry Lelegard
// BSD-2-Clause license, see LICENSE.txt file or https://tsduck.io/license
//
// The implementation of this module has been inspired by the excellent
// article at https://habr.com/en/articles/600123/
//
//----------------------------------------------------------------------------

#include "tsReactor.h"
#include "tsSysUtils.h"
#include "tsFatal.h"


//----------------------------------------------------------------------------
// Reactor constructors and destructor.
//----------------------------------------------------------------------------

ts::Reactor::Reactor(Report& report) :
    _report(report)
{
}

ts::Reactor::~Reactor()
{
    if (_is_open) {
        close();
    }
    assert(_resources.empty());
}


//----------------------------------------------------------------------------
// Reactor::HandlerInterface constructors and destructor.
//----------------------------------------------------------------------------

ts::Reactor::HandlerInterface::~HandlerInterface()
{
}


//----------------------------------------------------------------------------
// Reactor::Resource constructors and destructor.
//----------------------------------------------------------------------------

ts::Reactor::Resource::Resource() :
    canary(GOOD)
{
}

ts::Reactor::Resource::~Resource()
{
    // Detect use-after-free in case of incorrect order of usage.
    canary = BAD;
}


//----------------------------------------------------------------------------
// Allocate and register a new resource.
//----------------------------------------------------------------------------

ts::Reactor::Resource* ts::Reactor::newResource(EventType type, HandlerInterface* handler, HandlerMethod method)
{
    Resource* res = new Resource;
    CheckNonNull(res);
    res->type = type;
    res->handler = handler;
    res->method = method;
#if defined(TS_USE_IOCP)
    res->reactor = this;
#endif
    _resources.insert(res);
    return res;
}


//----------------------------------------------------------------------------
// Deregister and deallocate a resource.
//----------------------------------------------------------------------------

void ts::Reactor::deleteResource(Resource* res)
{
    if (res != nullptr) {
        _resources.erase(res);
        delete res;
    }
}


//----------------------------------------------------------------------------
// Check that a resource pointer is valid.
// Internal consistency check to detect incorrect understanding of system
// mechanisms leading to incorrect order of access to a Resource object.
//----------------------------------------------------------------------------

const ts::UChar* ts::Reactor::ResourceErrorMessage(Resource* res, EventType expected, EventType& actual)
{
    actual = EventType::NONE;
    if (res == nullptr) {
        return u"null pointer";
    }
    else {
        // Attempt memory access.
        try {
            if (res->canary != Resource::GOOD) {
                return res->canary == Resource::BAD ? u"reuse after free" : u"not a Resource block";
            }
            actual = res->type;
            if (expected != EventType::NONE && res->type != expected) {
                return u"inconsistent resource type";
            }
        }
        catch (...) {
            return u"invalid address";
        }
    }
    return nullptr;
}

bool ts::Reactor::validateResource(Resource* res, EventType type, bool display_errors)
{
    EventType evtype = EventType::NONE;
    const UChar* cause = ResourceErrorMessage(res, type, evtype);
    if (cause == nullptr && !_resources.contains(res)) {
        cause = u"Resource no longer in use in Reactor";
    }
    if (cause != nullptr) {
        const int severity = display_errors ? Severity::Error : Severity::Debug;
        static const UChar* const prefix = u"reactor internal error: invalid Resource pointer";
        if (type == EventType::NONE) {
            _report.log(severity, u"%s, %s", prefix, cause);
        }
        else {
            _report.log(severity, u"%s, %s, type: %s, expected: %s", prefix, cause, EventTypeNames().name(type), EventTypeNames().name(res->type));
        }
    }
    return cause == nullptr;
}


//----------------------------------------------------------------------------
// Types of source events.
//----------------------------------------------------------------------------

const ts::Names& ts::Reactor::EventTypeNames()
{
    static const Names data {
        {u"none",  EventType::NONE},
        {u"timer", EventType::TIMER},
        {u"event", EventType::EVENT},
    };
    return data;
}


//----------------------------------------------------------------------------
// Open and initialize the Reactor.
//----------------------------------------------------------------------------

bool ts::Reactor::open()
{
    if (_is_open) {
        _report.error(u"reactor already open");
        return false;
    }

    bool success = true;

#if defined(TS_USE_EPOLL)

    // Initialize the epoll event queue.
    assert(_epoll < 0);
    if ((_epoll = ::epoll_create1(EPOLL_CLOEXEC)) < 0) {
        _report.error(u"error creating epoll queue: %s", SysErrorCodeMessage());
        success = false;
    }
    AddCloseOnForkExec(_epoll, false);

#elif defined(TS_USE_KQUEUE)

    // Initialize the kernel queue.
    assert(_kqueue < 0);
    if ((_kqueue = ::kqueue()) < 0) {
        _report.error(u"error creating kqueue(): %s", SysErrorCodeMessage());
        success = false;
    }
    AddCloseOnForkExec(_kqueue);

#elif defined(TS_USE_IOCP)

    assert(_iocp == INVALID_HANDLE_VALUE);
    _iocp = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, 0);

    // CreateIoCompletionPort() is documented to return NULL on error but let's be conservative.
    if (_iocp == nullptr || _iocp == INVALID_HANDLE_VALUE) {
        _report.error(u"error in CreateIoCompletionPort(): %s", SysErrorCodeMessage());
        _iocp = INVALID_HANDLE_VALUE;
        success = false;
    }

#endif

    return _is_open = success;
}


//----------------------------------------------------------------------------
// Check that the reactor is open, check parameters.
//----------------------------------------------------------------------------

bool ts::Reactor::checkOpen()
{
    if (!_is_open) {
        _report.error(u"reactor is not initialized");
    }
    return _is_open;
}

bool ts::Reactor::checkHandler(HandlerInterface* handler, HandlerMethod method)
{
    const bool success = handler != nullptr && method != nullptr;
    if (!success) {
        _report.error(u"internal error: invalid event handler specified in reactor");
    }
    return success;
}


//----------------------------------------------------------------------------
// Close the Reactor.
//----------------------------------------------------------------------------

bool ts::Reactor::close()
{
    bool success = checkOpen();
    if (success) {
        // Force system close of pending resources, if there are any.
        for (auto res : _resources) {
            if (validateResource(res, EventType::NONE, true)) {
                if (res->type == EventType::EVENT) {
                    sysDeleteEvent(res, false);
                }
                else if (res->type == EventType::TIMER) {
                    sysDeleteTimer(res, false);
                }
                delete res;
            }
        }
        _resources.clear();

#if defined(TS_USE_EPOLL)

        // Cleanup epoll queue.
        assert(_epoll >= 0);
        RemoveCloseOnForkExec(_epoll);
        ::close(_epoll);
        _epoll = -1;

#elif defined(TS_USE_KQUEUE)

        // Cleanup kqueue().
        assert(_kqueue >= 0);
        RemoveCloseOnForkExec(_kqueue);
        ::close(_kqueue);
        _kqueue = -1;

#elif defined(TS_USE_IOCP)

        // Close the I/O completion port.
        assert(_iocp != INVALID_HANDLE_VALUE);
        ::CloseHandle(_iocp);
        _iocp = INVALID_HANDLE_VALUE;

#endif
    }

    _is_open = false;
    return success;
}


//----------------------------------------------------------------------------
// Exit processEventLoop() as soon as possible.
//----------------------------------------------------------------------------

void ts::Reactor::exitEventLoop(bool success)
{
    if (checkOpen()) {
        _exit_requested = true;
        if (!success) {
            _exit_success = false;
        }
    }
}


//----------------------------------------------------------------------------
// Process events until exit is requested.
//----------------------------------------------------------------------------

bool ts::Reactor::processEventLoop()
{
    if (!checkOpen()) {
        return false;
    }

    _exit_requested = false;
    _exit_success = true;

#if defined(TS_USE_EPOLL)

    // List of signalled events.
    std::vector<::epoll_event> events(MIN_WAIT_EVENTS);

    // Main event loop.
    while (!_exit_requested) {

        // Wait for events. No timeouts, use timers to timeout.
        adjustEventVector(events);
        const int status = ::epoll_wait(_epoll, &events[0], int(events.size()), -1);

        // In case of error...
        if (status < 0) {
            if (errno == EINTR) {
                // Interrupted by a signal, retry.
                continue;
            }
            else {
                _report.error(u"error while waiting for epoll: %s", SysErrorCodeMessage());
                _exit_requested = true;
                _exit_success = false;
                break;
            }
        }

        // Process all returned events.
        const size_t evcount = std::min(events.size(), size_t(status));
        for (size_t i = 0; i < evcount; i++) {

            // Get and check the associated Resource block.
            Resource* evres = reinterpret_cast<Resource*>(events[i].data.ptr);
            if (!validateResource(evres, EventType::NONE, true)) {
                continue;
            }
            const EventId id(evres);

            // Warning: resource can be invalidated if the user-handler updates the reactor. So, keep a copy of it.
            // If we need to erase the resource, we must use ires before calling the handler.
            const auto res(*evres);

            // Process the event, based on a copy of the Resource block.
            if (res.type == EventType::EVENT) {
                // Try to read from the event file descriptor to verify that the event is actually pending. Loop on EAGAIN.
                uint64_t data = 0;
                if (::read(res.fd, &data, sizeof(data)) > 0) {
                    // Successful read => the event was triggered. Call the user event callback.
                    (res.handler->*res.method)(*this, id);
                }
                else if (errno != EAGAIN) {
                    // Actual error, delete the event.
                    _report.error(u"error reading user event: %s", SysErrorCodeMessage());
                    sysDeleteEvent(evres, false);
                    deleteResource(evres);
                }
            }
            else if (res.type == EventType::TIMER) {
                // Try to read the number of expirations.
                uint64_t data = 0;
                if (::read(res.fd, &data, sizeof(data)) > 0) {
                    // Successful read => the timer expired.
                    // In case of one-shot event, the timer must be explicitly closed.
                    if (!res.repeat) {
                        sysDeleteTimer(evres, true);
                        deleteResource(evres);
                    }
                    // Call the timer callback.
                    (res.handler->*res.method)(*this, id);
                }
                else if (errno != EAGAIN) {
                    // Actual error, delete the timer.
                    _report.error(u"error reading timer: %s", SysErrorCodeMessage());
                    sysDeleteTimer(evres, false);
                    _resources.erase(evres);
                }
            }
            else {
                _report.debug(u"ignoring %s event signaled by epoll", EventTypeNames().name(res.type));
            }
        }
    }

#elif defined(TS_USE_KQUEUE)

    // List of signalled events.
    std::vector<kevent_t> events(MIN_WAIT_EVENTS);

    // Main event loop.
    while (!_exit_requested) {

        // Wait for events. No timeouts, use timers to timeout.
        adjustEventVector(events);
        const int status = ::kevent(_kqueue, nullptr, 0, &events[0], int(events.size()), nullptr);

        // In case of error...
        if (status < 0) {
            if (errno == EINTR) {
                // Interrupted by a signal, retry.
                continue;
            }
            else {
                _report.error(u"error while waiting for kevent: %s", SysErrorCodeMessage());
                _exit_requested = true;
                _exit_success = false;
                break;
            }
        }

        // Process all returned events.
        const size_t evcount = std::min(events.size(), size_t(status));
        for (size_t i = 0; i < evcount; i++) {

            // Get and check the associated Resource block.
            Resource* evres = reinterpret_cast<Resource*>(events[i].udata);
            if (!validateResource(evres, EventType::NONE, true)) {
                continue;
            }
            const EventId id(evres);

            // Warning: resource can be invalidated if the user-handler updates the reactor. So, keep a copy of it.
            // If we need to erase the resource, we must use ires before calling the handler.
            const auto res(*evres);

            // Process the event, based on a copy of the Resource block.
            if (res.type == EventType::EVENT) {
                // Call the user event callback.
                (res.handler->*res.method)(*this, id);
            }
            else if (res.type == EventType::TIMER) {
                // In case of one-shot event, the timer is removed from the kqueue().
                // We must manually remove it from the resource map before calling the handler.
                if (!res.repeat) {
                    deleteResource(evres);
                }
                // Call the timer callback.
                (res.handler->*res.method)(*this, id);
            }
            else {
                _report.debug(u"ignoring %s event signaled by kevent", EventTypeNames().name(res.type));
            }
        }
    }

#elif defined(TS_USE_IOCP)

    // List of signalled events.
    std::vector<::OVERLAPPED_ENTRY> events(MIN_WAIT_EVENTS);

    // Main event loop.
    while (!_exit_requested) {

        // Wait for events. No timeouts, use timers to timeout.
        // Important: specify to wait in alertable state (last parameter) to allow timer APC.
        adjustEventVector(events);
        ::ULONG retcount = 0;
        if (!::GetQueuedCompletionStatusEx(_iocp, &events[0], ::ULONG(events.size()), &retcount, INFINITE, true)) {
            const auto err = ::GetLastError();
            if (err == STATUS_USER_APC) {
                // The alertable state was interrupted by a user APC (here probably a timer completion).
                continue;
            }
            else {
                // Actual error.
                _report.error(u"error while waiting on I/O completion port: %s", SysErrorCodeMessage(err));
                _exit_requested = true;
                _exit_success = false;
                break;
            }
        }

        // Process all returned events.
        const size_t evcount = std::min(events.size(), size_t(retcount));
        for (size_t i = 0; i < evcount; i++) {

            // Get and check the associated Resource block.
            Resource* evres = reinterpret_cast<Resource*>(events[i].lpCompletionKey);
            if (!validateResource(evres, EventType::NONE, true)) {
                continue;
            }
            const EventId id(evres);

            // Warning: resource can be invalidated if the user-handler updates the reactor. So, keep a copy of it.
            const auto res(*evres);

            // Process the event, based on a copy of the Resource block.
            if (res.type == EventType::EVENT) {
                // Call the user event callback.
                (res.handler->*res.method)(*this, id);
            }
            else if (res.type == EventType::TIMER) {
                // In case of one-shot event, the timer must be explicitly closed.
                if (!res.repeat) {
                    sysDeleteTimer(evres, true);
                    deleteResource(evres);
                }
                // Call the timer callback.
                (res.handler->*res.method)(*this, id);
            }
            else {
                _report.debug(u"ignoring %s event signaled by GetQueuedCompletionStatusEx", EventTypeNames().name(res.type));
            }
        }
    }

#endif

    return _exit_success;
}


//----------------------------------------------------------------------------
// Add a timer.
//----------------------------------------------------------------------------

ts::Reactor::EventId ts::Reactor::newTimerImpl(HandlerInterface* handler, HandlerMethod method, cn::milliseconds duration, bool repeat)
{
    EventId id;

    // Check parameters.
    if (!checkOpen() || !checkHandler(handler, method)) {
        return id;
    }
    if (duration <= cn::milliseconds::zero()) {
        _report.error(u"invalid reactor timer value");
        return id;
    }

    // Create the resource block
    Resource* res = newResource(EventType::TIMER, handler, method);
    res->repeat = repeat;

#if defined(TS_USE_EPOLL)

    // On Linux, timers are file descriptors which can be monitored.
    res->fd = ::timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
    if (res->fd < 0) {
        _report.error(u"error in timerfd_create: %s", SysErrorCodeMessage());
        deleteResource(res);
        return id;
    }

    // Register time in epoll queue.
    ::epoll_event event;
    TS_ZERO(event);
    event.events = EPOLLIN | EPOLLET;
    event.data.ptr = res;
    if (::epoll_ctl(_epoll, EPOLL_CTL_ADD, res->fd, &event) < 0) {
        _report.error(u"error adding  timer to epoll: %s", SysErrorCodeMessage());
        ::close(res->fd);
        deleteResource(res);
        return id;
    }

    // Time definition of the timer.
    ::itimerspec its;
    TS_ZERO(its);
    its.it_value.tv_sec = timespec_sec_t(cn::duration_cast<cn::seconds>(duration).count());
    its.it_value.tv_nsec = timespec_nsec_t(cn::duration_cast<cn::nanoseconds>(duration).count() % cn::nanoseconds::period::den);
    if (repeat) {
        // Repeat at same interval as initial timeout.
        its.it_interval = its.it_value;
    }
    if (::timerfd_settime(res->fd, 0, &its, nullptr) < 0) {
        _report.error(u"error in timerfd_settime: %s", SysErrorCodeMessage());
        ::epoll_ctl(_epoll, EPOLL_CTL_DEL, res->fd, nullptr);
        ::close(res->fd);
        deleteResource(res);
        return id;
    }

    // Make sure the file descriptor is closed on fork().
    AddCloseOnForkExec(res->fd, false);

#elif defined(TS_USE_KQUEUE)

    // With kqueue(), the timers ids are arbitrary values. Register the event in the kqueue.
    // In kevent, the default value for timer is in milliseconds.
    kevent_t ev;
    EV_SET(&ev, kevent_ident_t(res), EVFILT_TIMER, EV_ADD | EV_ENABLE | (repeat ? 0 : EV_ONESHOT), 0, kevent_data_t(duration.count()), res);
    if (::kevent(_kqueue, &ev, 1, nullptr, 0, nullptr) < 0) {
        _report.error(u"error registering timer in kqueue: %s", SysErrorCodeMessage());
        deleteResource(res);
        return id;
    }

#elif defined(TS_USE_IOCP)

    // Create a waitable timer.
    res->handle = ::CreateWaitableTimerW(nullptr, true, nullptr);
    if (res->handle == nullptr || res->handle == INVALID_HANDLE_VALUE) {
        _report.error(u"error creating waitable timer: %s", SysErrorCodeMessage());
        deleteResource(res);
        return id;
    }

    // Build the timer expiration time: Use 100 nanosecond intervals.
    // - Positive values indicate UTC absolute time in the format described by FILETIME.
    // - Negative values indicate relative time.
    ::LARGE_INTEGER expiration;
    expiration.QuadPart = -(duration.count() * 10'000); // 10,000 100-ns units = 1 ms

    // Set the timer exîration.
    if (!::SetWaitableTimer(res->handle, &expiration, ::ULONG(repeat ? duration.count() : 0), Reactor::WinTimerCompletion, res, false)) {
        _report.error(u"error setting waitable timer: %s", SysErrorCodeMessage());
        ::CloseHandle(res->handle);
        deleteResource(res);
        return id;
    }

#endif

    // Timer is finally valid.
    id._res = res;
    return id;
}


//----------------------------------------------------------------------------
// Cancel a timer.
//----------------------------------------------------------------------------

bool ts::Reactor::cancelTimer(EventId id, bool display_errors)
{
    // Check parameters.
    if (!checkOpen() || !validateResource(id._res, EventType::TIMER, display_errors)) {
        return false;
    }

    // Cancel and delete the timer.
    bool success = sysDeleteTimer(id._res, display_errors);
    deleteResource(id._res);
    return success;
}

bool ts::Reactor::sysDeleteTimer(Resource* res, bool display_errors)
{
    bool success = true;

#if defined(TS_USE_EPOLL)

    // Remove timer from epoll queue.
    if (::epoll_ctl(_epoll, EPOLL_CTL_DEL, res->fd, nullptr) < 0) {
        _report.log(display_errors ? Severity::Error : Severity::Debug, u"error removing timer from epoll: %s", SysErrorCodeMessage());
        success = false;
    }
    RemoveCloseOnForkExec(res->fd);
    ::close(res->fd);

#elif defined(TS_USE_KQUEUE)

    // Delete the timer from the kqueue.
    kevent_t ev;
    EV_SET(&ev, kevent_ident_t(res), EVFILT_TIMER, EV_DELETE | EV_DISABLE, 0, 0, res);
    if (::kevent(_kqueue, &ev, 1, nullptr, 0, nullptr) < 0) {
        _report.log(display_errors ? Severity::Error : Severity::Debug, u"error deleting user event in kqueue: %s", SysErrorCodeMessage());
        success = false;
    }

#elif defined(TS_USE_IOCP)

    // Cancel the timer if pending. Ignore errors (timer may not be pending).
    // Note that CancelWaitableTimer cancels outstanding APCs.
    // Therefore, WinTimerCompletion won't be called on this one, even is already pending.
    ::CancelWaitableTimer(res->handle);
    ::CloseHandle(res->handle);
    res->handle = INVALID_HANDLE_VALUE;

#endif

    return success;
}


//----------------------------------------------------------------------------
// Windows-specific: Asynchronous timer completion routine.
//----------------------------------------------------------------------------

#if defined(TS_USE_IOCP)

void ts::Reactor::WinTimerCompletion(::LPVOID arg, ::DWORD timer_low, ::DWORD time_high)
{
    // We are in asynchronous context, don't access the Reactor structures, don't display errors.
    Resource* res = reinterpret_cast<Resource*>(arg);
    EventType type = EventType::NONE;
    if (ResourceErrorMessage(res, EventType::TIMER, type) == nullptr) {
        // Push a notification using the resource block.
        ::PostQueuedCompletionStatus(res->reactor->_iocp, 0, ::ULONG_PTR(res), nullptr);
    }
}

#endif

//----------------------------------------------------------------------------
// Add a user event in the reactor.
//----------------------------------------------------------------------------

ts::Reactor::EventId ts::Reactor::newEventImpl(HandlerInterface* handler, HandlerMethod method)
{
    EventId id;

    // Check parameters.
    if (!checkOpen() || !checkHandler(handler, method)) {
        return id;
    }

    // Create the resource block
    Resource* res = newResource(EventType::EVENT, handler, method);

#if defined(TS_USE_EPOLL)

    // Open a event file descriptor.
    res->fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (res->fd < 0) {
        _report.error(u"error creating user event with eventfd(): %s", SysErrorCodeMessage());
        deleteResource(res);
        return id;
    }

    // Register event in epoll queue.
    ::epoll_event event;
    TS_ZERO(event);
    event.events = EPOLLIN | EPOLLET;
    event.data.ptr = res;
    if (::epoll_ctl(_epoll, EPOLL_CTL_ADD, res->fd, &event) < 0) {
        _report.error(u"error adding user event to epoll: %s", SysErrorCodeMessage());
        ::close(res->fd);
        deleteResource(res);
        return id;
    }

    // Make sure the file descriptor is closed on fork().
    AddCloseOnForkExec(res->fd, false);

#elif defined(TS_USE_KQUEUE)

    // With kqueue(), the user-defined events are arbitrary values. Register the event in the kqueue.
    // Note: user data is nullptr here, it is non-null on trigger only.
    kevent_t ev;
    EV_SET(&ev, kevent_ident_t(res), EVFILT_USER, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, nullptr);
    if (::kevent(_kqueue, &ev, 1, nullptr, 0, nullptr) < 0) {
        _report.error(u"error registering user event in kqueue: %s", SysErrorCodeMessage());
        deleteResource(res);
        return id;
    }

#elif defined(TS_USE_IOCP)

    // An event has no specific system handle. The event is explicitly pushed in the I/O completion queue.

#endif

    // Event is finally valid.
    id._res = res;
    return id;
}


//----------------------------------------------------------------------------
// Signal a user event in the reactor.
// Important: this method can be invoked from any thread.
//----------------------------------------------------------------------------

bool ts::Reactor::signalEvent(EventId id)
{
    bool success = true;

    // Check parameters. However, do not try to access the _resources map because
    // we can be invoked from another thread. So, don't try to check that the Id
    // is actually a user event.
    if (!id.isValid()) {
        _report.error(u"internal error: invalid id in Reactor::signalEvent");
        return false;
    }

#if defined(TS_USE_EPOLL)

    // Write the 64-bit value 1 in the event file descriptor.
    uint64_t value = 1;
    if (::write(id._res->fd, &value, sizeof(value)) < 0) {
        _report.error(u"error triggering user event in epoll: %s", SysErrorCodeMessage());
        success = false;
    }

#elif defined(TS_USE_KQUEUE)

    // Simple trigger, only use the kqueue file descriptor, don't do anything else.
    kevent_t ev;
    EV_SET(&ev, kevent_ident_t(id._res), EVFILT_USER, 0, NOTE_TRIGGER, 0, id._res);
    if (::kevent(_kqueue, &ev, 1, nullptr, 0, nullptr) < 0) {
        _report.error(u"error triggering user event in kqueue: %s", SysErrorCodeMessage());
        success = false;
    }

#elif defined(TS_USE_IOCP)

    // Push a notification using the resource block.
    if (!::PostQueuedCompletionStatus(_iocp, 0, ::ULONG_PTR(id._res), nullptr)) {
        _report.error(u"error triggering user event in PostQueuedCompletionStatus: %s", SysErrorCodeMessage());
        success = false;
    }

#endif

    return success;
}


//----------------------------------------------------------------------------
// Delete a user event.
//----------------------------------------------------------------------------

bool ts::Reactor::deleteEvent(EventId id)
{
    // Check parameters.
    if (!checkOpen() || !validateResource(id._res, EventType::EVENT, true)) {
        return false;
    }

    // Delete the event.
    bool success = sysDeleteEvent(id._res, true);
    deleteResource(id._res);
    return success;
}

bool ts::Reactor::sysDeleteEvent(Resource* res, bool display_errors)
{
    bool success = true;

#if defined(TS_USE_EPOLL)

    // Remove event from epoll queue.
    if (::epoll_ctl(_epoll, EPOLL_CTL_DEL, res->fd, nullptr) < 0) {
        _report.log(display_errors ? Severity::Error : Severity::Debug, u"error removing user event from epoll: %s", SysErrorCodeMessage());
        success = false;
    }
    RemoveCloseOnForkExec(res->fd);
    ::close(res->fd);

#elif defined(TS_USE_KQUEUE)

    // Delete the event from the kqueue.
    kevent_t ev;
    EV_SET(&ev, kevent_ident_t(res), EVFILT_USER, EV_DELETE | EV_DISABLE, 0, 0, nullptr);
    if (::kevent(_kqueue, &ev, 1, nullptr, 0, nullptr) < 0) {
        _report.log(display_errors ? Severity::Error : Severity::Debug, u"error deleting user event in kqueue: %s", SysErrorCodeMessage());
        success = false;
    }

#elif defined(TS_USE_IOCP)

    // Nothing to close at system level.

#endif

    return success;
}
