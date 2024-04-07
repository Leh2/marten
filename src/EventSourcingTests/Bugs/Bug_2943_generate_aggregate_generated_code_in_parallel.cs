using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventSourcingTests.Projections.CodeGeneration;
using JasperFx.CodeGeneration;
using JasperFx.Core.Reflection;
using Marten;
using Marten.Events.Aggregation;
using Marten.Events.Projections;
using Marten.Internal.CodeGeneration;
using Marten.Testing.Harness;
using Shouldly;
using Xunit;

namespace EventSourcingTests.Bugs;

public class Bug_2943_generate_aggregate_generated_code_in_parallel
{
    [Fact]
    public void aggregates_do_not_fail_code_generation_on_parallel_execution()
    {
        var options = new StoreOptions();
        options.Connection(ConnectionSource.ConnectionString);

        // Given
        options.Projections.LiveStreamAggregation<ProjectionCodeGenerationTests.Something>();

        // When
        var store = new DocumentStore(options);
        Parallel.For(1, 100, _ =>
        {
            Parallel.ForEach(store.Events.As<ICodeFileCollection>().BuildFiles().OfType<IProjectionSource>(), projection =>
            {
                projection.Build(store);
            });
        });

        // Then
        store.Events.As<ICodeFileCollection>().BuildFiles()
            .OfType<SingleStreamProjection<ProjectionCodeGenerationTests.Something>>()
            .ShouldHaveSingleItem();

        options.BuildFiles()
            .OfType<DocumentProviderBuilder>()
            .Where(e => e.ProviderName == typeof(ProjectionCodeGenerationTests.Something).ToSuffixedTypeName("Provider"))
            .ShouldHaveSingleItem();
    }

    [Fact]
    public async Task aggregates_do_not_fail_code_generation_on_parallel_FetchForWriting_execution()
    {
        var options = new StoreOptions();
        options.Connection(ConnectionSource.ConnectionString);

        // Given
        options.Projections.LiveStreamAggregation<ProjectionCodeGenerationTests.Something>();

        // When
        var store = new DocumentStore(options);

        await store.Storage.ApplyAllConfiguredChangesToDatabaseAsync().ConfigureAwait(false);


        Parallel.For(1, 100, _ =>
        {
            store.LightweightSession().Events.FetchForWriting<ProjectionCodeGenerationTests.Something>(Guid.NewGuid()).GetAwaiter().GetResult();
        });

        // Then
        store.Events.As<ICodeFileCollection>().BuildFiles()
            .OfType<SingleStreamProjection<ProjectionCodeGenerationTests.Something>>()
            .ShouldHaveSingleItem();

        options.BuildFiles()
            .OfType<DocumentProviderBuilder>()
            .Where(e => e.ProviderName == typeof(ProjectionCodeGenerationTests.Something).ToSuffixedTypeName("Provider"))
            .ShouldHaveSingleItem();
    }

    [Fact]
    public async Task aggregates_do_not_fail_code_generation_on_parallel_various_operations()
    {
        var commonId = new Guid("b1f3b3b1-0b1b-4b1b-8b1b-2b1b1b1b1b1b");
        var options = new StoreOptions();
        options.Connection(ConnectionSource.ConnectionString);
        options.Projections.Snapshot<Projector>(SnapshotLifecycle.Inline);
        options.GeneratedCodeMode = TypeLoadMode.Auto;
        options.SetApplicationProject(typeof(Projector).Assembly);
        options.SourceCodeWritingEnabled = true;
        // var seedStore = new DocumentStore(options);
        // var seedSession = seedStore.LightweightSession();
        // seedSession.Events.Append(commonId, new EventOne(commonId), new EventOne(commonId));
        // await seedSession.SaveChangesAsync();

        Console.WriteLine("HERE WE GO");
        var store = new DocumentStore(options);
        var exceptions = new List<Exception>();
        var actions = new List<Func<Task>>
        {
            () =>
            {
                Console.WriteLine($"{Thread.CurrentThread.ManagedThreadId}: FIRST STEP IS RUNNING");
                var session2 = store.LightweightSession();
                session2.Events.Append(commonId, new EventOne(commonId), new EventOne(commonId));
                return session2.SaveChangesAsync();
            },
            () =>
            {
                Console.WriteLine($"{Thread.CurrentThread.ManagedThreadId}: SECOND STEP IS WAITING");
                Task.Delay(3000).GetAwaiter().GetResult();
                Console.WriteLine($"{Thread.CurrentThread.ManagedThreadId}: SECOND STEP IS RUNNUING");
                var session = store.LightweightSession();
                return session.Events.AggregateStreamAsync<Projector>(commonId);
            },
        };
        await Parallel.ForEachAsync(actions, async (action, _) =>
        {
            try
            {
                await action();
            }
            catch (Exception e)
            {
                exceptions.Add(e);
            }
        });

        exceptions.ShouldBeEmpty();
    }
}

public class Projector
{
    public Guid Id { get; set; }
    public IList<EventOne> Events { get; set; } = new List<EventOne>();

    private void Apply(EventOne e)
    {
        Events.Add(e);
    }
}

public class EventOne(Guid id)
{
    public Guid Id { get; set; } = id;
}
