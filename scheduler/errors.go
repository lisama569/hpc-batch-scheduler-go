package scheduler

import "errors"

// ErrNotFound 表示作业/节点/队列不存在。
var ErrNotFound = errors.New("scheduler: not found")

// ErrInvalidState 表示操作对象的当前状态不允许该操作。
var ErrInvalidState = errors.New("scheduler: invalid state")

// ErrAlreadyExists 表示同名对象已存在。
var ErrAlreadyExists = errors.New("scheduler: already exists")
