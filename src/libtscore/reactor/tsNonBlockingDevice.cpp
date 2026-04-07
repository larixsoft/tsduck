//----------------------------------------------------------------------------
//
// TSDuck - The MPEG Transport Stream Toolkit
// Copyright (c) 2005-2026, Thierry Lelegard
// BSD-2-Clause license, see LICENSE.txt file or https://tsduck.io/license
//
//----------------------------------------------------------------------------

#include "tsNonBlockingDevice.h"


//----------------------------------------------------------------------------
// Destructor.
//----------------------------------------------------------------------------

ts::NonBlockingDevice::~NonBlockingDevice()
{
}

//----------------------------------------------------------------------------
// Set the device in non-blocking mode.
//----------------------------------------------------------------------------

bool ts::NonBlockingDevice::setNonBlocking(bool non_blocking)
{
    if (allowSetNonBlocking()) {
        _is_non_blocking = non_blocking;
        return true;
    }
    else {
        return false;
    }
}

bool ts::NonBlockingDevice::allowSetNonBlocking() const
{
    // The default implementation always allows setting the non-blocking mode.
    return true;
}


//----------------------------------------------------------------------------
// Check the blocking mode of a device.
//----------------------------------------------------------------------------

bool ts::NonBlockingDevice::checkNonBlocking(bool non_blocking, const UChar* opname, Report& report) const
{
    if (non_blocking == _is_non_blocking) {
        return true;
    }
    else {
        report.error(u"internal error: %s called in %sblocking mode", opname, non_blocking ? u"" : u"non-");
        return false;
    }
}

bool ts::NonBlockingDevice::checkNonBlocking(bool* would_block, const UChar* opname, Report& report) const
{
    if (would_block != nullptr) {
        *would_block = false;
    }
    return checkNonBlocking(would_block != nullptr, opname, report);
}
