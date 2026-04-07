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
    if (auto_open) {
        UDPSocket::open(gen, _reactor.report());
    }
}


//----------------------------------------------------------------------------
// Destructor
//----------------------------------------------------------------------------

ts::ReactiveUDPSocket::~ReactiveUDPSocket()
{
}
