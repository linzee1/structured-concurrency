package io.github.huatalk.parallelinscope.scope;

/** Terminal reason for one member of a parallel task group. */
public enum TaskGroupMemberReason {
    SUCCESS,
    USER_FAILURE,
    SUBMISSION_FAILURE,
    MEMBER_CANCELED,
    GROUP_CANCELED,
    FAIL_FAST,
    TIMEOUT
}
