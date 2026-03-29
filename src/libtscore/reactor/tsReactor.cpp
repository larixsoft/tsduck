//----------------------------------------------------------------------------
//
// TSDuck - The MPEG Transport Stream Toolkit
// Copyright (c) 2005-2026, Thierry Lelegard
// BSD-2-Clause license, see LICENSE.txt file or https://tsduck.io/license
//
// The implementation of this module has been inspired by the excellent
// article at https://habr.com/en/articles/600123/
//
// TODO: With epoll and kqueue, make sure that file descriptors are properly
// closed on fork() or exec().
//
//----------------------------------------------------------------------------

#include "tsReactor.h"
#include "tsNames.h"
#include "tsSysUtils.h"
#include "tsFatal.h"

//
// Depending on the operating system, we use distinct forms of kernel event queues.
//
#if defined(TS_LINUX)
    // Use epoll(), a Linux specific feature.
    // Future: investigate io_uring.
    #define TS_USE_EPOLL 1
#elif defined(TS_MAC) || defined(TS_BSD)
    // Use kqueue(), as found on macOS and all BSD systems.
    #define TS_USE_KQUEUE 1
#elif defined(TS_WINDOWS)
    // Use I/O completion ports
    #define TS_USE_IOCP 1
#else
    #error "unsupported operating system"
#endif

#if defined(TS_USE_EPOLL)
    #include "tsBeforeStandardHeaders.h"
    #include <sys/epoll.h>
    #include <sys/eventfd.h>
    #include <sys/timerfd.h>
    #include "tsAfterStandardHeaders.h"
#elif defined(TS_USE_KQUEUE)
    #include "tsBeforeStandardHeaders.h"
    #include <sys/types.h>
    #include <sys/event.h>
    #include <sys/time.h>
    #include "tsAfterStandardHeaders.h"
#endif


//----------------------------------------------------------------------------
// Internal guts.
//----------------------------------------------------------------------------

class ts::Reactor::Guts
{
    TS_NOBUILD_NOCOPY(Guts);
public:
    // Constructor & destructor.
    Guts(Reactor& reactor, Report& report);
    ~Guts();

    // Delegated methods from Reactor class.
    Report& report() { return _report; }
    bool open();
    bool close();
    bool isOpen() const { return _is_open; }
    bool processEventLoop();
    void exitEventLoop(bool success);
    EventId newTimerImpl(HandlerInterface* handler, HandlerMethod method, cn::milliseconds duration, bool repeat);
    bool cancelTimer(EventId id, bool display_errors);
    EventId newEventImpl(HandlerInterface* handler, HandlerMethod method);
    bool signalEvent(EventId id);
    bool deleteEvent(EventId id);

private:
    // Types of source events.
    enum class EventType {NONE, TIMER, EVENT};
    static const Names& EventTypeNames();

    // Description of one resource to wait for.
    class Resource
    {
        TS_DEFAULT_COPY_MOVE(Resource);
    public:
        Resource() = default;

        EventType         type = EventType::NONE;
        HandlerInterface* handler = nullptr;
        HandlerMethod     method = nullptr;
        bool              repeat = false;     // for timers
    };

    // Map of active resources by id.
    using ResourceMap = std::map<EventId, Resource>;

    // Reactor private members.
    Reactor&    _reactor;
    Report&     _report;
    bool        _is_open = false;
    bool        _exit_requested = false;  // Exit event loop when possible.
    bool        _exit_success = true;     // Exit status for event loop.
    ResourceMap _resources {};            // Resource to wait for.

    // Check that the reactor is open, check parameters.
    bool checkOpen();
    bool checkHandler(HandlerInterface* handler, HandlerMethod method);

    // Register a new resource, if valid. Return nullptr if invalid.
    Resource* registerResource(EventId id, EventType type, HandlerInterface* handler, HandlerMethod method);

    // Check that a resource exists with the given type. Return nullptr on error.
    Resource* getResource(EventId id, EventType type, bool display_errors = true);

    // Close the system part of a resource.
    bool sysDeleteTimer(EventId id, Resource& res, bool display_errors = true);
    bool sysDeleteEvent(EventId id, Resource& res, bool display_errors = true);

#if defined(TS_USE_EPOLL)

    int _epoll = -1;  // File descriptor for epoll().

#elif defined(TS_USE_KQUEUE)

    using kevent_t = struct kevent;
    using kevent_ident_t = decltype(kevent_t::ident);
    using kevent_data_t = decltype(kevent_t::data);

    int            _kqueue = -1;        // File descriptor for kqueue(). Can be used in another thread (signal event).
    kevent_ident_t _ident_counter = 0;  // Allocator for timer and event ids.

    // Allocate a new id for a timer or event. Set the msb to avoid conflict with file descriptor values.
    kevent_ident_t newUserId() { return _ident_counter++ | (kevent_ident_t(1) << (8 * sizeof(kevent_ident_t) - 1)); }

#endif
};


//----------------------------------------------------------------------------
// Reactor constructors and destructor.
//----------------------------------------------------------------------------

ts::Reactor::Reactor(Report& report) :
    _guts(new Guts(*this, report))
{
    CheckNonNull(_guts);
}

ts::Reactor::~Reactor()
{
    if (_guts != nullptr) {
        delete _guts;
        _guts = nullptr;
    }
}


//----------------------------------------------------------------------------
// Delegations to internal guts.
//----------------------------------------------------------------------------

ts::Report& ts::Reactor::report()
{
    return _guts->report();
}
bool ts::Reactor::open()
{
    return _guts->open();
}
bool ts::Reactor::close()
{
    return _guts->close();
}
bool ts::Reactor::isOpen() const
{
    return _guts->isOpen();
}
void ts::Reactor::exitEventLoop(bool success)
{
    _guts->exitEventLoop(success);
}
bool ts::Reactor::processEventLoop()
{
    return _guts->processEventLoop();
}
ts::Reactor::EventId ts::Reactor::newTimerImpl(HandlerInterface* handler, HandlerMethod method, cn::milliseconds duration, bool repeat)
{
    return _guts->newTimerImpl(handler, method, duration, repeat);
}
bool ts::Reactor::cancelTimer(EventId id, bool display_errors)
{
    return _guts->cancelTimer(id, display_errors);
}
ts::Reactor::EventId ts::Reactor::newEventImpl(HandlerInterface* handler, HandlerMethod method)
{
    return _guts->newEventImpl(handler, method);
}
bool ts::Reactor::signalEvent(EventId id)
{
    return _guts->signalEvent(id);
}
bool ts::Reactor::deleteEvent(EventId id)
{
    return _guts->deleteEvent(id);
}


//----------------------------------------------------------------------------
// Reactor::Guts constructors and destructor.
//----------------------------------------------------------------------------

ts::Reactor::Guts::Guts(Reactor& reactor, Report& report) :
    _reactor(reactor),
    _report(report)
{
}

ts::Reactor::Guts::~Guts()
{
    if (_is_open) {
        close();
    }
}


//----------------------------------------------------------------------------
// Reactor::HandlerInterface constructors and destructor.
//----------------------------------------------------------------------------

ts::Reactor::HandlerInterface::~HandlerInterface()
{
}


//----------------------------------------------------------------------------
// Types of source events.
//----------------------------------------------------------------------------

const ts::Names& ts::Reactor::Guts::EventTypeNames()
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

bool ts::Reactor::Guts::open()
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

#elif defined(TS_USE_KQUEUE)

    // Initialize the kernel queue.
    assert(_kqueue < 0);
    if ((_kqueue = ::kqueue()) < 0) {
        _report.error(u"error creating kqueue(): %s", SysErrorCodeMessage());
        success = false;
    }

#elif defined(TS_USE_IOCP)

    //@@@

#endif

    return _is_open = success;
}


//----------------------------------------------------------------------------
// Close the Reactor.
//----------------------------------------------------------------------------

bool ts::Reactor::Guts::close()
{
    bool success = checkOpen();
    if (success) {
        // Force system close of pending resources, if there are any.
        for (auto it : _resources) {
            if (it.second.type == EventType::EVENT) {
                sysDeleteEvent(it.first, it.second, false);
            }
            else if (it.second.type == EventType::TIMER) {
                sysDeleteTimer(it.first, it.second, false);
            }
        }
        _resources.clear();

#if defined(TS_USE_EPOLL)

        // Cleanup epoll queue.
        assert(_epoll >= 0);
        ::close(_epoll);
        _epoll = -1;

#elif defined(TS_USE_KQUEUE)

        // Cleanup kqueue().
        assert(_kqueue >= 0);
        ::close(_kqueue);
        _kqueue = -1;

#elif defined(TS_USE_IOCP)

        //@@@

#endif
    }
    _is_open = false;
    return success;
}


//----------------------------------------------------------------------------
// Check that the reactor is open, check parameters.
//----------------------------------------------------------------------------

bool ts::Reactor::Guts::checkOpen()
{
    if (!_is_open) {
        _report.error(u"reactor is not initialized");
    }
    return _is_open;
}

bool ts::Reactor::Guts::checkHandler(HandlerInterface* handler, HandlerMethod method)
{
    const bool success = handler != nullptr && method != nullptr;
    if (!success) {
        _report.error(u"internal error: invalid event handler specified");
    }
    return success;
}


//----------------------------------------------------------------------------
// Exit processEventLoop() as soon as possible.
//----------------------------------------------------------------------------

void ts::Reactor::Guts::exitEventLoop(bool success)
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

bool ts::Reactor::Guts::processEventLoop()
{
    if (!checkOpen()) {
        return false;
    }

    _exit_requested = false;
    _exit_success = true;

    // We try to avoid too many reallocations by keeping some minimal number of events.
    [[maybe_unused]] static constexpr size_t min_events = 16;

#if defined(TS_USE_EPOLL)

    // List of signalled events.
    std::vector<::epoll_event> events(min_events);

    // Main event loop.
    while (!_exit_requested) {

        // Make provision for all expected events.
        if (events.size() < _resources.size() || events.size() > _resources.size() / 5) {
            events.resize(_resources.size() + min_events);
        }

        // Wait for events. No timeouts, use timers to timeout.
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
            const EventId id(events[i].data.fd);
            const auto ires = _resources.find(id);
            if (ires == _resources.end()) {
                _report.debug(u"ignoring unknown or obsolete event %d signaled by epoll", id._id);
                continue;
            }
            // Warning: res can be invalidated if the user-handler updates the reactor. So, keep a copy of it.
            const auto res(ires->second);
            if (res.type == EventType::EVENT) {
                // Try to read from the event file descriptor to verify that the event is actually pending. Loop on EAGAIN.
                uint64_t data = 0;
                for (;;) {
                    if (::read(int(id._id), &data, sizeof(data)) > 0) {
                        // Successful read => the event was triggered.
                        // Call the user event callback.
                        (res.handler->*res.method)(_reactor, id);
                    }
                    else if (errno != EAGAIN) {
                        break;
                    }
                }
            }
            else if (res.type == EventType::TIMER) {
                /****@@@@
                // In case of one-shot event, the timer is removed from the kqueue().
                // We must manually remove it from the resource map before calling the handler.
                if (!res.repeat) {
                    _resources.erase(ires);
                }
                // Call the timer callback.
                (res.handler->*res.method)(_reactor, id);
                @@@@@@*/
            }
            else {
                _report.debug(u"ignoring %s event %d signaled by epoll", EventTypeNames().name(res.type), id._id);
            }
        }
    }

#elif defined(TS_USE_KQUEUE)

    // List of signalled events.
    std::vector<kevent_t> events(min_events);

    // Main event loop.
    while (!_exit_requested) {

        // Make provision for all expected events.
        if (events.size() < _resources.size() || events.size() > _resources.size() / 5) {
            events.resize(_resources.size() + min_events);
        }

        // Wait for events. No timeouts, use timers to timeout.
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
            const EventId id(events[i].ident);
            const auto ires = _resources.find(id);
            if (ires == _resources.end()) {
                _report.debug(u"ignoring unknown or obsolete event %d signaled by kevent", id._id);
                continue;
            }
            // Warning: res can be invalidated if the user-handler updates the reactor. So, keep a copy of it.
            const auto res(ires->second);
            if (res.type == EventType::EVENT) {
                // Call the user event callback.
                (res.handler->*res.method)(_reactor, id);
            }
            else if (res.type == EventType::TIMER) {
                // In case of one-shot event, the timer is removed from the kqueue().
                // We must manually remove it from the resource map before calling the handler.
                if (!res.repeat) {
                    _resources.erase(ires);
                }
                // Call the timer callback.
                (res.handler->*res.method)(_reactor, id);
            }
            else {
                _report.debug(u"ignoring %s event %d signaled by kevent", EventTypeNames().name(res.type), id._id);
            }
        }
    }

#elif defined(TS_USE_IOCP)

    //@@@

#endif

    return _exit_success;
}


//----------------------------------------------------------------------------
// Register a new resource, if valid.
//----------------------------------------------------------------------------

ts::Reactor::Guts::Resource* ts::Reactor::Guts::registerResource(EventId id, EventType type, HandlerInterface* handler, HandlerMethod method)
{
    Resource* res = nullptr;
    if (id.isValid()) {
        // This should be a new id.
        // Currently only emit a warning and return the existing resource.
        if (_resources.contains(id)) {
            _report.warning(u"duplicate %s id", EventTypeNames().name(type));
        }

        // Initialize the resource.
        res = &_resources[id];
        res->type = type;
        res->handler = handler;
        res->method = method;
    }
    return res;
}


//----------------------------------------------------------------------------
// Check that a resource exists with the given type.
//----------------------------------------------------------------------------

ts::Reactor::Guts::Resource* ts::Reactor::Guts::getResource(EventId id, EventType type, bool display_errors)
{
    Resource* res = nullptr;
    if (!id.isValid()) {
        _report.log(display_errors ? Severity::Error : Severity::Debug, u"invalid %s id", EventTypeNames().name(type));
    }
    else {
        auto ires = _resources.find(id);
        if (ires == _resources.end()) {
            _report.log(display_errors ? Severity::Error : Severity::Debug, u"%s id no longer exists", EventTypeNames().name(type));
        }
        else if (ires->second.type != type) {
            _report.log(display_errors ? Severity::Error : Severity::Debug, u"inconsistent resource id, expected a %s, got a %s",
                        EventTypeNames().name(type), EventTypeNames().name(ires->second.type));
        }
        else {
            res = &ires->second;
        }
    }
    return res;
}


//----------------------------------------------------------------------------
// Add a timer.
//----------------------------------------------------------------------------

ts::Reactor::EventId ts::Reactor::Guts::newTimerImpl(HandlerInterface* handler, HandlerMethod method, cn::milliseconds duration, bool repeat)
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

#if defined(TS_USE_EPOLL)

    //@@@

#elif defined(TS_USE_KQUEUE)

    // With kqueue(), the timers are arbitrary values.
    id._id = EventId::Id(newUserId());

    // Register the event in the kqueue.
    // In kevent, the default value for timer is in milliseconds.
    kevent_t ev;
    EV_SET(&ev, kevent_ident_t(id._id), EVFILT_TIMER, EV_ADD | EV_ENABLE | (repeat ? 0 : EV_ONESHOT), 0, kevent_data_t(duration.count()), nullptr);
    if (::kevent(_kqueue, &ev, 1, nullptr, 0, nullptr) < 0) {
        _report.error(u"error registering timer in kqueue: %s", SysErrorCodeMessage());
        id.invalidate();
    }

#elif defined(TS_USE_IOCP)

    //@@@

#endif

    // Register the resource on success (ie. if id is valid).
    Resource* res = registerResource(id, EventType::TIMER, handler, method);
    if (res != nullptr) {
        res->repeat = repeat;
    }
    return id;
}


//----------------------------------------------------------------------------
// Cancel a timer.
//----------------------------------------------------------------------------

bool ts::Reactor::Guts::cancelTimer(EventId id, bool display_errors)
{
    // Check parameters.
    if (!checkOpen()) {
        return false;
    }
    Resource* res = getResource(id, EventType::TIMER, display_errors);
    if (res == nullptr) {
        return false;
    }

    // Cancel and delete the timer.
    bool success = sysDeleteTimer(id, *res, display_errors);
    _resources.erase(id);
    return success;
}

bool ts::Reactor::Guts::sysDeleteTimer(EventId id, Resource& res, bool display_errors)
{
    bool success = true;

#if defined(TS_USE_EPOLL)

    //@@@

#elif defined(TS_USE_KQUEUE)

    // Delete the timer from the kqueue.
    kevent_t ev;
    EV_SET(&ev, kevent_ident_t(id._id), EVFILT_TIMER, EV_DELETE | EV_DISABLE, 0, 0, nullptr);
    if (::kevent(_kqueue, &ev, 1, nullptr, 0, nullptr) < 0) {
        _report.log(display_errors ? Severity::Error : Severity::Debug, u"error deleting user event in kqueue: %s", SysErrorCodeMessage());
        success = false;
    }

#elif defined(TS_USE_IOCP)

    //@@@

#endif

    return success;
}


//----------------------------------------------------------------------------
// Add a user event in the reactor.
//----------------------------------------------------------------------------

ts::Reactor::EventId ts::Reactor::Guts::newEventImpl(HandlerInterface* handler, HandlerMethod method)
{
    EventId id;

    // Check parameters.
    if (!checkOpen() || !checkHandler(handler, method)) {
        return id;
    }

#if defined(TS_USE_EPOLL)

    // Open a event file descriptor.
    const int evfd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (evfd < 0) {
        _report.error(u"error creating user event with eventfd(): %s", SysErrorCodeMessage());
        return id;
    }

    // Register event in epoll queue.
    ::epoll_event event;
    TS_ZERO(event);
    event.events = EPOLLIN | EPOLLET;
    if (::epoll_ctl(_epoll, EPOLL_CTL_ADD, evfd, &event) < 0) {
        _report.error(u"error adding  user event to epoll: %s", SysErrorCodeMessage());
        ::close(evfd);
        return id;
    }

    // The event is now fully registered.
    id._id = EventId::Id(evfd);

#elif defined(TS_USE_KQUEUE)

    // With kqueue(), the user-defined events are arbitrary values.
    id._id = EventId::Id(newUserId());

    // Register the event in the kqueue.
    kevent_t ev;
    EV_SET(&ev, kevent_ident_t(id._id), EVFILT_USER, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, nullptr);
    if (::kevent(_kqueue, &ev, 1, nullptr, 0, nullptr) < 0) {
        _report.error(u"error registering user event in kqueue: %s", SysErrorCodeMessage());
        id.invalidate();
    }

#elif defined(TS_USE_IOCP)

    //@@@

#endif

    // Register the resource on success (ie. if id is valid).
    registerResource(id, EventType::EVENT, handler, method);
    return id;
}


//----------------------------------------------------------------------------
// Signal a user event in the reactor.
// Important: this method can be invoked from any thread.
//----------------------------------------------------------------------------

bool ts::Reactor::Guts::signalEvent(EventId id)
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
    if (::write(int(id._id), &value, sizeof(value)) < 0) {
        _report.error(u"error triggering user event in epoll: %s", SysErrorCodeMessage());
        success = false;
    }

#elif defined(TS_USE_KQUEUE)

    // Simple trigger, only use the kqueue file descriptor, don't do anything else.
    kevent_t ev;
    EV_SET(&ev, kevent_ident_t(id._id), EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);
    if (::kevent(_kqueue, &ev, 1, nullptr, 0, nullptr) < 0) {
        _report.error(u"error triggering user event in kqueue: %s", SysErrorCodeMessage());
        success = false;
    }

#elif defined(TS_USE_IOCP)

    //@@@

#endif

    return success;
}


//----------------------------------------------------------------------------
// Delete a user event.
//----------------------------------------------------------------------------

bool ts::Reactor::Guts::deleteEvent(EventId id)
{
    // Check parameters.
    if (!checkOpen()) {
        return false;
    }
    Resource* res = getResource(id, EventType::EVENT);
    if (res == nullptr) {
        return false;
    }

    // Delete the event.
    bool success = sysDeleteEvent(id, *res);
    _resources.erase(id);
    return success;
}

bool ts::Reactor::Guts::sysDeleteEvent(EventId id, Resource& res, bool display_errors)
{
    bool success = true;

#if defined(TS_USE_EPOLL)

    // Remove event from epoll queue.
    if (::epoll_ctl(_epoll, EPOLL_CTL_DEL, int(id._id), nullptr) < 0) {
        _report.log(display_errors ? Severity::Error : Severity::Debug, u"error removing user event from epoll: %s", SysErrorCodeMessage());
        success = false;
    }
    ::close(int(id._id));

#elif defined(TS_USE_KQUEUE)

    // Delete the event from the kqueue.
    kevent_t ev;
    EV_SET(&ev, kevent_ident_t(id._id), EVFILT_USER, EV_DELETE | EV_DISABLE, 0, 0, nullptr);
    if (::kevent(_kqueue, &ev, 1, nullptr, 0, nullptr) < 0) {
        _report.log(display_errors ? Severity::Error : Severity::Debug, u"error deleting user event in kqueue: %s", SysErrorCodeMessage());
        success = false;
    }

#elif defined(TS_USE_IOCP)

    //@@@

#endif

    return success;
}
