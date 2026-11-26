/*
 * *****************************************************************************
 * Copyright (C) 2014-2026 Dennis Sheirer
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 * ****************************************************************************
 */

package io.github.dsheirer.health;

import java.util.HashMap;
import java.util.Map;

/**
 * What became of each call a control channel announced, for a traffic channel manager that keeps no tracker of its
 * own per call (DMR): the current call on each channel (frequency and timeslot) and its outcome so far, moved in
 * the control channel's CallTally as it changes, so every call is counted once, by what finally became of it.
 *
 * Not thread safe: the traffic channel manager calls it under its lock.
 */
public class CallOutcomes
{
    private final CallTally mTally;
    private final Map<String,CallOutcome> mCalls = new HashMap<>();

    /**
     * @param tally of the control channel
     */
    public CallOutcomes(CallTally tally)
    {
        mTally = tally;
    }

    /**
     * The key of the call on a channel.
     * @param frequency of the channel (or, without one, its channel number)
     * @param timeslot of the call, 0 when the channel has none
     */
    public static String key(long frequency, int timeslot)
    {
        return frequency + "/" + timeslot;
    }

    /**
     * Counts a call as it stands now.
     * @param key of the channel the call is on
     * @param newCall true for a call just announced (any earlier call on the channel has ended)
     * @param outcome of the call now
     */
    public void tally(String key, boolean newCall, CallOutcome outcome)
    {
        if(key == null || outcome == null)
        {
            return;
        }

        //A grant for a call this hasn't seen start (it started before the control channel did) is a new call.
        CallOutcome previous = newCall || !mCalls.containsKey(key) ? null : mCalls.get(key);

        if(previous != outcome)
        {
            mTally.count(previous, outcome);
        }

        mCalls.put(key, outcome);
    }

    /**
     * No tuner could take the traffic channel started for the call on this channel: a call counted as recorded is
     * counted as noSource instead.
     * @param key of the channel the traffic channel was started for, or null (nothing to move)
     */
    public void rejected(String key)
    {
        if(key != null && mCalls.get(key) == CallOutcome.RECORDED)
        {
            mTally.count(CallOutcome.RECORDED, CallOutcome.NO_SOURCE);
            mCalls.put(key, CallOutcome.NO_SOURCE);
        }
    }

    /**
     * The outcome so far of the call on a channel, or null when there's none.
     */
    public CallOutcome get(String key)
    {
        return mCalls.get(key);
    }

    /**
     * The traffic channel on a frequency stopped: the calls on its timeslots are forgotten (each was counted already),
     * so a later rejection on the frequency can't move one of them.  Not on the manager's stop: a Capacity Plus
     * manager carries on with the next rest channel, its traffic channels still running.
     * @param frequency of the traffic channel
     */
    public void forget(long frequency)
    {
        String prefix = frequency + "/";
        mCalls.keySet().removeIf(key -> key.startsWith(prefix));
    }
}
