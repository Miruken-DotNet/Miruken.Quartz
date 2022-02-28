namespace Miruken.Quartz;

using System;

[AttributeUsage(AttributeTargets.Method, Inherited = false)]
public class ScheduleAttribute : Attribute
{
    public string ScheduleKey { get; set; }
    public string Group       { get; set; }
}