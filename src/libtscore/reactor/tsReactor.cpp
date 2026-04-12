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

ts::Reactor::Resource* ts::Reactor::newResource(EventType type, ReactorHandlerInterface* handler)
{
    Resource* res = new Resource;
    CheckNonNull(res);
    res->type = type;
    res->handler = handler;
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
        static const UChar prefix[] = u"reactor internal error: invalid Resource pointer";
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
        {u"read",  EventType::READ},
        {u"write", EventType::WRITE},
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
    // Unlike kqueue, the epoll file descriptor is inherited by a child created with fork().
    // Register it to be closed in child process after fork().
    AddCloseOnForkExec(_epoll, false);

#elif defined(TS_USE_KQUEUE)

    // Initialize the kernel queue.
    // Unlike epoll, the kqueue file descriptor is not inherited by a child created with fork().
    assert(_kqueue < 0);
    if ((_kqueue = ::kqueue()) < 0) {
        _report.error(u"error creating kqueue(): %s", SysErrorCodeMessage());
        success = false;
    }

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
// Error checking.
//----------------------------------------------------------------------------

bool ts::Reactor::checkOpen()
{
    if (!_is_open) {
        _report.error(u"reactor is not initialized");
    }
    return _is_open;
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
                switch (res->type) {
                    case EventType::EVENT:
                        sysDeleteEvent(res, false);
                        break;
                    case EventType::TIMER:
                        sysDeleteTimer(res, false);
                        break;
                    case EventType::READ:
                        sysDeleteRead(res, false);
                        break;
                    case EventType::WRITE:
                        sysDeleteWrite(res, false);
                        break;
                    case EventType::NONE:
                    default:
                        break;
                }
                // Free the resource block only if it was validated. Cases where an invalid resource block remains
                // in a reactor are bugs. In that case, we don't try to delete it (a memory leak is still better
                // than a potential double-free which may lead to memory corruption).
                delete res;
            }
            else {
                // Invalid resource block, errors already displayed.
                success = false;
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

    // List of signalled events. The type SysEventStructure depends on the operating system.
    std::vector<SysEventStructure> events(MIN_WAIT_EVENTS);

    // Main event loop.
    while (!_exit_requested) {

        // Adjust the size of the system structures describing events to wait for.
        if (events.size() < _resources.size() || events.size() > _resources.size() + 10 * MIN_WAIT_EVENTS) {
            // Current vector is too small or significantly too large.
            events.resize(_resources.size() + MIN_WAIT_EVENTS);
        }

        // Number of valid returned events.
        size_t evcount = 0;

        // Wait for events. No timeouts, use timers to timeout.
        #if defined(TS_USE_EPOLL)

            const int status = ::epoll_wait(_epoll, &events[0], int(events.size()), -1);
            if (status >= 0) {
                evcount = std::min(events.size(), size_t(status));
            }
            else if (errno == EINTR) {
                // Interrupted by a signal, retry event loop.
                continue;
            }
            else {
                _report.error(u"error while waiting for epoll: %s", SysErrorCodeMessage());
                _exit_requested = true;
                _exit_success = false;
                break;
            }

        #elif defined(TS_USE_KQUEUE)

            const int status = ::kevent(_kqueue, nullptr, 0, &events[0], int(events.size()), nullptr);
            if (status >= 0) {
                evcount = std::min(events.size(), size_t(status));
            }
            else if (errno == EINTR) {
                // Interrupted by a signal, retry event loop.
                continue;
            }
            else {
                _report.error(u"error while waiting for kevent: %s", SysErrorCodeMessage());
                _exit_requested = true;
                _exit_success = false;
                break;
            }

        #elif defined(TS_USE_IOCP)

            // Important: specify to wait in alertable state (last parameter) to allow timer APC.
            ::ULONG retcount = 0;
            if (::GetQueuedCompletionStatusEx(_iocp, &events[0], ::ULONG(events.size()), &retcount, INFINITE, true)) {
                evcount = std::min(events.size(), size_t(retcount));
            }
            else if (::GetLastError() == STATUS_USER_APC) {
                // The alertable state was interrupted by a user APC (here probably a timer completion), retry event loop.
                continue;
            }
            else {
                // Actual error.
                _report.error(u"error while waiting on I/O completion port: %s", SysErrorCodeMessage());
                _exit_requested = true;
                _exit_success = false;
                break;
            }

        #endif

        // Process all returned events.
        for (size_t i = 0; i < evcount && !_exit_requested; i++) {

            // Get and check the associated Resource block.
            auto& ev(events[i]);
            Resource* evres = GetResource(ev);
            if (!validateResource(evres, EventType::NONE, true)) {
                continue;
            }
            const EventId id(evres);

            // Warning: resource can be invalidated if the user-handler updates the reactor. So, keep a copy of it.
            // If we need to erase the resource, we must use ires before calling the handler.
            const auto res(*evres);

            // Process the event, based on a copy of the Resource block.
            bool success = true;
            switch (res.type) {

                case EventType::EVENT: {
                    #if defined(TS_USE_EPOLL)
                        // Try to read from the event file descriptor to verify that the event is actually pending. Loop on EAGAIN.
                        uint64_t data = 0;
                        success = ::read(res.fd, &data, sizeof(data)) > 0;
                        if (!success && errno != EAGAIN) {
                            // Actual error, delete the event.
                            _report.error(u"error reading user event: %s", SysErrorCodeMessage());
                            sysDeleteEvent(evres, false);
                            deleteResource(evres);
                        }
                    #endif
                    if (success) {
                        // Call the user event callback.
                        ReactorEventHandlerInterface* handler = dynamic_cast<ReactorEventHandlerInterface*>(res.handler);
                        if (handler != nullptr) {
                            handler->handleUserEvent(*this, id);
                        }
                    }
                    break;
                }

                case EventType::TIMER: {
                    #if defined(TS_USE_EPOLL)
                        // Try to read the number of expirations.
                        uint64_t data = 0;
                        success = ::read(res.fd, &data, sizeof(data)) > 0;
                        if (!success && errno != EAGAIN) {
                            // Actual error, delete the timer.
                            _report.error(u"error reading timer: %s", SysErrorCodeMessage());
                            sysDeleteTimer(evres, false);
                            deleteResource(evres);
                        }
                    #endif
                    if (success) {
                        if (!res.repeat) {
                            // In case of one-shot event, the timer must be explicitly closed.
                            // Except with kqueue() where one-shot timers are automatically disarmed.
                            #if !defined(TS_USE_KQUEUE)
                                sysDeleteTimer(evres, true);
                            #endif
                            // Manually remove the one-shot timer from the resource map before calling the handler.
                            deleteResource(evres);
                        }
                        // Call the timer callback.
                        ReactorTimerHandlerInterface* handler = dynamic_cast<ReactorTimerHandlerInterface*>(res.handler);
                        if (handler != nullptr) {
                            handler->handleTimer(*this, id);
                        }
                    }
                    break;
                }

                case EventType::READ: {
                    size_t io_size = NPOS;
                    int error_code = 0;

                    #if defined(TS_USE_EPOLL)
                        //@@@
                    #elif defined(TS_USE_KQUEUE)
                        // If the event shall not be repeated, disable it, but keep the filter in the kqueue.
                        if (!res.repeat) {
                            kevent_t kev;
                            EV_SET(&kev, kevent_ident_t(res.fd), EVFILT_READ, EV_DISABLE, 0, 0, evres);
                            if (::kevent(_kqueue, &kev, 1, nullptr, 0, nullptr) < 0) {
                                _report.error(u"error disabling read event in kqueue: %s", SysErrorCodeMessage());
                            }
                        }
                        // The data field contains either the pending I/O size or error code.
                        if (ev.flags & EV_ERROR) {
                            error_code = int(ev.data);
                        }
                        else {
                            io_size = size_t(ev.data);
                        }
                    #elif defined(TS_USE_IOCP)
                        //@@@
                    #endif

                    // Call the I/O callback.
                    ReactorReadHandlerInterface* handler = dynamic_cast<ReactorReadHandlerInterface*>(res.handler);
                    if (handler != nullptr) {
                        handler->handleReadEvent(*this, id, io_size, error_code);
                    }
                    break;
                }

                case EventType::WRITE: {
                    size_t io_size = NPOS;
                    int error_code = 0;

                    #if defined(TS_USE_EPOLL)
                        //@@@
                    #elif defined(TS_USE_KQUEUE)
                        // If the event shall not be repeated, disable it, but keep the filter in the kqueue.
                        if (!res.repeat) {
                            kevent_t kev;
                            EV_SET(&kev, kevent_ident_t(res.fd), EVFILT_WRITE, EV_DISABLE, 0, 0, evres);
                            if (::kevent(_kqueue, &kev, 1, nullptr, 0, nullptr) < 0) {
                                _report.error(u"error disabling write event in kqueue: %s", SysErrorCodeMessage());
                            }
                        }
                        // The data field contains either the remaining output buffer size or error code.
                        // We cannot really use the output buffer size, so ignore it.
                        if (ev.flags & EV_ERROR) {
                            error_code = int(ev.data);
                        }
                    #elif defined(TS_USE_IOCP)
                        //@@@
                    #endif

                    // Call the I/O callback.
                    ReactorWriteHandlerInterface* handler = dynamic_cast<ReactorWriteHandlerInterface*>(res.handler);
                    if (handler != nullptr) {
                        handler->handleWriteEvent(*this, id, io_size, error_code);
                    }
                    break;
                }

                case EventType::NONE:
                default: {
                    _report.debug(u"ignoring %s event signaled", EventTypeNames().name(res.type));
                    break;
                }
            }
        }
    }

    return _exit_success;
}


//----------------------------------------------------------------------------
// Add a timer.
//----------------------------------------------------------------------------

ts::EventId ts::Reactor::newTimerImpl(ReactorTimerHandlerInterface* handler, cn::milliseconds duration, bool repeat)
{
    EventId id;

    // Check parameters.
    if (!checkOpen()) {
        return id;
    }
    if (duration <= cn::milliseconds::zero()) {
        _report.error(u"invalid reactor timer value");
        return id;
    }

    // Create the resource block
    Resource* res = newResource(EventType::TIMER, handler);
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
    Resource* res = reinterpret_cast<Resource*>(id._res);
    if (!checkOpen() || !validateResource(res, EventType::TIMER, display_errors)) {
        return false;
    }

    // Cancel and delete the timer.
    bool success = sysDeleteTimer(res, display_errors);
    deleteResource(res);
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

ts::EventId ts::Reactor::newEvent(ReactorEventHandlerInterface* handler)
{
    EventId id;

    // Check parameters.
    if (!checkOpen()) {
        return id;
    }

    // Create the resource block
    Resource* res = newResource(EventType::EVENT, handler);

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
    Resource* res = reinterpret_cast<Resource*>(id._res);
    if (::write(res->fd, &value, sizeof(value)) < 0) {
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
    Resource* res = reinterpret_cast<Resource*>(id._res);
    if (!checkOpen() || !validateResource(res, EventType::EVENT, true)) {
        return false;
    }

    // Delete the event.
    bool success = sysDeleteEvent(res, true);
    deleteResource(res);
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


//----------------------------------------------------------------------------
// Add in the reactor a notification of read-ready or read-completion.
//----------------------------------------------------------------------------

ts::EventId ts::Reactor::newReadNotify(ReactorReadHandlerInterface* handler, SysSocketType sock, bool repeat)
{
    EventId id;

    // Check parameters.
    if (!checkOpen()) {
        return id;
    }

    // Create the resource block
    Resource* res = newResource(EventType::READ, handler);
    res->repeat = repeat;

#if defined(TS_USE_EPOLL)

    //@@@

#elif defined(TS_USE_KQUEUE)

    // If repeat is false, we must not use EV_ONESHOT. This would delete the filter from the kqueue.
    // Instead, we will use EV_DISABLE when the event is fired.
    kevent_t ev;
    EV_SET(&ev, kevent_ident_t(sock), EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, res);
    if (::kevent(_kqueue, &ev, 1, nullptr, 0, nullptr) < 0) {
        _report.error(u"error registering read event in kqueue: %s", SysErrorCodeMessage());
        deleteResource(res);
        return id;
    }
    res->fd = sock;

#elif defined(TS_USE_IOCP)

    //@@@

#endif

    // Event is finally valid.
    id._res = res;
    return id;
}


//----------------------------------------------------------------------------
// Rearm a previous one-shot read-ready or read-completion.
//----------------------------------------------------------------------------

bool ts::Reactor::rearmReadNotify(EventId id, bool repeat)
{
    // Check parameters.
    Resource* res = reinterpret_cast<Resource*>(id._res);
    if (!checkOpen() || !validateResource(res, EventType::READ, true)) {
        return false;
    }

    bool success = true;

#if defined(TS_USE_EPOLL)

    //@@@

#elif defined(TS_USE_KQUEUE)

    kevent_t ev;
    EV_SET(&ev, kevent_ident_t(res->fd), EVFILT_READ, EV_ENABLE, 0, 0, res);
    if (::kevent(_kqueue, &ev, 1, nullptr, 0, nullptr) < 0) {
        _report.error(u"error rearming read event in kqueue: %s", SysErrorCodeMessage());
        return false;
    }
    res->repeat = repeat;

#elif defined(TS_USE_IOCP)

    //@@@

#endif

    return success;
}


//----------------------------------------------------------------------------
// Delete a notification of read-ready or read-completion.
//----------------------------------------------------------------------------

bool ts::Reactor::deleteReadNotify(EventId id)
{
    // Check parameters.
    Resource* res = reinterpret_cast<Resource*>(id._res);
    if (!checkOpen() || !validateResource(res, EventType::READ, true)) {
        return false;
    }

    // Delete the event.
    bool success = sysDeleteRead(res, true);
    deleteResource(res);
    return success;
}

bool ts::Reactor::sysDeleteRead(Resource* res, bool display_errors)
{
    bool success = true;

#if defined(TS_USE_EPOLL)

    //@@@@

#elif defined(TS_USE_KQUEUE)

    kevent_t ev;
    EV_SET(&ev, kevent_ident_t(res->fd), EVFILT_READ, EV_DELETE | EV_DISABLE, 0, 0, nullptr);
    if (::kevent(_kqueue, &ev, 1, nullptr, 0, nullptr) < 0) {
        _report.log(display_errors ? Severity::Error : Severity::Debug, u"error deleting read event in kqueue: %s", SysErrorCodeMessage());
        success = false;
    }

#elif defined(TS_USE_IOCP)

    //@@@@

#endif

    return success;
}


//----------------------------------------------------------------------------
// Add in the reactor a notification of write-ready or read-completion.
//----------------------------------------------------------------------------

ts::EventId ts::Reactor::newWriteNotify(ReactorWriteHandlerInterface* handler, SysSocketType sock, bool repeat)
{
    EventId id;

    // Check parameters.
    if (!checkOpen()) {
        return id;
    }

    // Create the resource block
    Resource* res = newResource(EventType::WRITE, handler);
    res->repeat = repeat;

#if defined(TS_USE_EPOLL)

    //@@@

#elif defined(TS_USE_KQUEUE)

    // If repeat is false, we must not use EV_ONESHOT. This would delete the filter from the kqueue.
    // Instead, we will use EV_DISABLE when the event is fired.
    kevent_t ev;
    EV_SET(&ev, kevent_ident_t(sock), EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, res);
    if (::kevent(_kqueue, &ev, 1, nullptr, 0, nullptr) < 0) {
        _report.error(u"error registering write event in kqueue: %s", SysErrorCodeMessage());
        deleteResource(res);
        return id;
    }
    res->fd = sock;

#elif defined(TS_USE_IOCP)

    //@@@

#endif

    // Event is finally valid.
    id._res = res;
    return id;
}


//----------------------------------------------------------------------------
// Rearm a previous one-shot write-ready or write-completion.
//----------------------------------------------------------------------------

bool ts::Reactor::rearmWriteNotify(EventId id, bool repeat)
{
    // Check parameters.
    Resource* res = reinterpret_cast<Resource*>(id._res);
    if (!checkOpen() || !validateResource(res, EventType::WRITE, true)) {
        return false;
    }

    bool success = true;

#if defined(TS_USE_EPOLL)

    //@@@

#elif defined(TS_USE_KQUEUE)

    kevent_t ev;
    EV_SET(&ev, kevent_ident_t(res->fd), EVFILT_WRITE, EV_ENABLE, 0, 0, res);
    if (::kevent(_kqueue, &ev, 1, nullptr, 0, nullptr) < 0) {
        _report.error(u"error rearming read event in kqueue: %s", SysErrorCodeMessage());
        return false;
    }
    res->repeat = repeat;

#elif defined(TS_USE_IOCP)

    //@@@

#endif

    return success;
}


//----------------------------------------------------------------------------
// Delete a notification of write-ready or write-completion.
//----------------------------------------------------------------------------

bool ts::Reactor::deleteWriteNotify(EventId id)
{
    // Check parameters.
    Resource* res = reinterpret_cast<Resource*>(id._res);
    if (!checkOpen() || !validateResource(res, EventType::WRITE, true)) {
        return false;
    }

    // Delete the event.
    bool success = sysDeleteWrite(res, true);
    deleteResource(res);
    return success;
}

bool ts::Reactor::sysDeleteWrite(Resource* res, bool display_errors)
{
    bool success = true;

#if defined(TS_USE_EPOLL)

    //@@@@

#elif defined(TS_USE_KQUEUE)

    kevent_t ev;
    EV_SET(&ev, kevent_ident_t(res->fd), EVFILT_WRITE, EV_DELETE | EV_DISABLE, 0, 0, nullptr);
    if (::kevent(_kqueue, &ev, 1, nullptr, 0, nullptr) < 0) {
        _report.log(display_errors ? Severity::Error : Severity::Debug, u"error deleting write event in kqueue: %s", SysErrorCodeMessage());
        success = false;
    }

#elif defined(TS_USE_IOCP)

    //@@@@

#endif

    return success;
}
