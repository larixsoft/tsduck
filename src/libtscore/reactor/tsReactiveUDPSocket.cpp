//----------------------------------------------------------------------------
//
// TSDuck - The MPEG Transport Stream Toolkit
// Copyright (c) 2005-2026, Thierry Lelegard
// BSD-2-Clause license, see LICENSE.txt file or https://tsduck.io/license
//
//----------------------------------------------------------------------------

#include "tsReactiveUDPSocket.h"


//----------------------------------------------------------------------------
// Constructors and destructor.
//----------------------------------------------------------------------------

ts::ReactiveUDPSocket::ReactiveUDPSocket(Reactor& reactor, bool auto_open, IP gen) :
    _reactor(reactor)
{
#if defined(TS_UNIX)
    setNonBlocking(true);
#endif
    setDefaultReport(&reactor.report(), false);
    if (auto_open) {
        UDPSocket::open(gen, _reactor.report());
    }
}

ts::ReactiveUDPSocket::~ReactiveUDPSocket()
{
}


//----------------------------------------------------------------------------
// Disable overridden synchronous UDPSocket methods.
//----------------------------------------------------------------------------

bool ts::ReactiveUDPSocket::send(const void* data, size_t size, const IPSocketAddress& destination, Report& report, bool* would_block)
{
    report.error(u"cannot call synchronous send() in reactive UDP socket");
    return false;
}

bool ts::ReactiveUDPSocket::send(const void* data, size_t size, Report& report, bool* would_block)
{
    report.error(u"cannot call synchronous send() in reactive UDP socket");
    return false;
}

bool ts::ReactiveUDPSocket::receive(void* data, size_t max_size, size_t& ret_size, IPSocketAddress& sender, IPSocketAddress& destination,
                                    const AbortInterface* abort, Report& report, cn::microseconds* timestamp, TimeStampType* timestamp_type, bool* would_block)
{
    report.error(u"cannot call synchronous receive() in reactive UDP socket");
    return false;
}
