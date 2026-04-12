//----------------------------------------------------------------------------
//
// TSDuck - The MPEG Transport Stream Toolkit
// Copyright (c) 2005-2026, Thierry Lelegard
// BSD-2-Clause license, see LICENSE.txt file or https://tsduck.io/license
//
//----------------------------------------------------------------------------
//!
//!  @file
//!  UDP Socket for use in a Reactor environment
//!
//----------------------------------------------------------------------------

#pragma once
#include "tsUDPSocket.h"
#include "tsReactor.h"

namespace ts {
    //!
    //! UDP Socket for use in a Reactor environment.
    //! @ingroup libtscore reactor
    //!
    class TSCOREDLL ReactiveUDPSocket: public UDPSocket
    {
        TS_NOBUILD_NOCOPY(ReactiveUDPSocket);
    public:
        //!
        //! Constructor.
        //! @param [in,out] reactor Associated reactor. The reactor must remain valid as long as this object is valid.
        //! @param [in] auto_open If true, call open() immediately.
        //! @param [in] gen IP generation, IPv4 or IPv6.
        //! If set to IP::Any, this socket can receive IPv4 and IPv6 datagrams.
        //! If @a gen is IP::v6, the socket is created with option IPV6_V6ONLY set.
        //!
        ReactiveUDPSocket(Reactor& reactor, bool auto_open = false, IP gen = IP::Any);

        //!
        //! Destructor.
        //!
        virtual ~ReactiveUDPSocket() override;

        // Override synchronous UDPSocket methods, just to disable them (return an error).
        virtual bool send(const void* data, size_t size, const IPSocketAddress& destination, Report& report = CERR, bool* would_block = nullptr) override;
        virtual bool send(const void* data, size_t size, Report& report = CERR, bool* would_block = nullptr) override;
        virtual bool receive(void* data,
                             size_t max_size,
                             size_t& ret_size,
                             IPSocketAddress& sender,
                             IPSocketAddress& destination,
                             const AbortInterface* abort = nullptr,
                             Report& report = CERR,
                             cn::microseconds* timestamp = nullptr,
                             TimeStampType* timestamp_type = nullptr,
                             bool* would_block = nullptr) override;

    private:
        Reactor& _reactor;
    };
}
