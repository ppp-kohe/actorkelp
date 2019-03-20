# actorlib 

実験用actor実装

## Implementation note

* 基本的にakkaの単純化

* `Actor`, `ActorRef`

    *   `Actor`が`ActorRef`を実装する
    * `Actor`は`Mailbox`と`ActorSystem`を持ち、`tell(obj, sender)`の実行ごとに`Message`を作って`ActorSystem`に投げる

* `ActorSystem`: ローカル実装の`ActorSystemDefault`

    * `send(Message)`で送られたメッセージを対象の`Actor`を解決し、 
        * その`Actor` の`Mailbox`に`offer(msg)`で追加するとともに、
        * スレッドプールに`processMessageSubsequently`で対象アクターの`Mailbox`にあるメッセージの一定数を実行するタスクを起動
        * ローカルでメッセージが送られた分だけタスクが起動するので全てのメッセージが処理されるはず

* `Actor`の`processMessage(Message)`が実際のメッセージ処理

    * `ActorDefault`が`Actor`のサブクラスで、`ActorBehavior`を持つ: receiveの処理をする

    * `ActorBehaviorBuilder`で`ActorBehavior`を作成できる

        * `ActorDefault`では`initBehavior ` をoverrideし`behaviorBuilder()`で構築する

    * ```java
        class MyActor extends ActorDefault {
            MyActor(ActorSystem system) {
                super(system);
            }
            @Override
            protected ActorBehavior initBehavior() {
                return behaviorBuilder()
                    .match(String.class, this::recv)
                    .build();
            }
            
            void recv(String m) {
                System.out.println(m);
            }
        }
        ```

* `Actor`にはStringの名前を設定可能: `ActorSystem`の`register(Actor)`で登録し、 `ActorRef`である  ` ActorRefLocalNamed` により解決できる

    * remoteのためには名前をつける必要がある
    * //TODO unregister, actor shutdown

* `remote`パッケージ

    * `ActorSystemRemote` : `ActorSystem`のリモート版で、`ActorSystemDefault`をラップ

    * 通信にはnettyを使う

        * `ObjectMessageServer ` : 受信
            * `EventLoopGroup` やport番号, `receiver`を設定し`start()`により`ServerBootstrap`を作って起動する
            * `start()`で`close`されるまでブロック
            * `startWithoutWait()`で ノンブロッキングで起動 //TODO 要改善
            * `childHandler`: `SeverInitializer`
                * `ChannelPipleline`にハンドラを追加。 ドキュメントを見ると同じパイプラインに追加しているように見えて`Inbound`と`Outbound`の型により入出力を区別し、さらにOutboundだと処理順が逆になる
                * `LoggingHandler`: `LogLevel`を設定しないとログが出ない
                * `LengthFieldBasedFrameDecoder` : decorderは分割されて受信するパケットをまとめてくれる。このdecorderはヘッダとして4バイトの本体データのサイズを付加する
                * `QueueServerHandler`: `Inbound`。nettyのバッファである`ByteBuf`のデータをオブジェクトとして復元。`receiver`を呼び出して渡す。 レスポンスとして`200`をintの値として書き込む
                    * `exceptionCaught`のoverrideが重要な模様
                    * //TODO releaseが必要?
                    * 書き込みにはkryoの`Input`を`ByteBufInputStream`で`ByteBuf`をラップして作る
        * `ObjectMessageClient ` : 送信
            * サーバーと同様の起動方法だが、複数の接続先に`ObjectMessageConnection`で対応する。
            * `connect().setHost(h).setPort(p)`で設定し、`write(Object)`で`ChannelFuture`をopenする。
            * `LengthFieldPrepender`で4バイトの長さを読み取る。 encoder
            * `QueueClientHandler`: encoder。書き込みには`Output`を`ByteBufOutputStream`でラップ
            * `ResponseHandler`: 送信レスポンス `200`を読み取る

    * `ActorSystemDefaultForRemote`: スレッド数を決める

        * プロセッサの約1.5倍のスレッド
        * おそらくスレッドプールは共有しないほうがいい
        * ローカルのプール数は procs / 2= availableProcessors() / 2
        * 受信用の`EventLoopGroup`は1+procs/2
        * 送信用の`EventLoopGroup`はprocs/2

    * `ActorRemoteSystem#send(Message)`で送るときは、

        * まず`message.getTarget()`で対象をとり`ActorRefRemote`だった場合、その`ActorAddress`に対応する`ConnectionActor`を作る
            * これはキャッシュする
            * 内部で`ObjectMessageConnection`を保持
            * このactorに`tell(message, null)`をする: すると`Message(ConnectionActor,null,Message)`となる
            * このactorの`processMessage(Message)`は中の`Message` を取り出し`ObjectMessageConnection`に書き込みを行う
            * //TODO client自体でEventLoopGroupにより複数スレッド持つのでactorにするのは無駄か

    * シリアライゼーションにはkryoを使う

        * `KryoBuilder`: 利用すると思われるデータのクラスを片っ端から登録する
            * (元はakka対応のコードなのだが、全ての登録の必要性がakkaのためだか覚えてない)
        * `ActorRefRemoteSerializer` : `ActorRef`のシリアライズ

    * `ActorRefRemote`は`ActorAddress`を持つ

        * `ActorAddress`はホスト、ポートの`ActorAddressRemote`と、そのホスト、ポート、アクター名の`ActorAddressRemoteActor`どちらかで、`ActorRefRemote`は後者でないと意味がない
            * `ActorAddressRemote`は`ActorRemoteSystem`が自身の名前として保持する
        * シリアライズでは`Actor`、`ActorRefLocalNamed`は`ActorSystemRemote`から自身のアドレスを取り出してアクター名と組み合わせ`ActorAddress`として書き出す
            * 復元は単に`ActorRefRemote`にする。転送された時点で別のホストなのでこれでよく、ローカルに戻すのは`ActorSystemRemote#localize`で明示的に行う

    * `ActorSystemRemote#recieve(Object)`: 受信した場合に呼び出される

        * `Message`に反応し、対象を`localize(ActorRef)`してメッセージを作り直してローカルで実行する
        * `localze`は`ActorAddressRemoteActor`の名前から`ActorRefLocalNamed`を作る。host名は見ない

        