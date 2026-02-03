using BepInEx;
using BepInEx.Configuration;
using HarmonyLib;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Emit;
using UnityEngine;

namespace MyGlobalSignalTower
{
    [BepInPlugin("com.myself.globalsignaltower", "Global Signal Tower Ultimate", "1.5.0")]
    public class GlobalSignalTowerMod : BaseUnityPlugin
    {
        internal static BepInEx.Logging.ManualLogSource Log;

        // 配置项
        public static ConfigEntry<float> Cfg_PowerConnect;
        public static ConfigEntry<float> Cfg_PowerCover;
        public static ConfigEntry<float> Cfg_GroundSignalRange;
        public static ConfigEntry<float> Cfg_SpaceSignalRange;
        public static ConfigEntry<float> Cfg_BattleBasePickRange;
        public static ConfigEntry<float> Cfg_BattleBaseConstructRange;
        public static ConfigEntry<float> Cfg_GaussTurretAttackRange; // 同时也控制等离子炮射程
        public static ConfigEntry<float> Cfg_PlasmaMaxSpeed;
        public static ConfigEntry<bool> Cfg_EnableMarkAll;
        public static ConfigEntry<bool> Cfg_EnablePlasmaTurretPatch;

        // 运行时变量
        private static int lastStarId = -1;
        private static int update_frame_counter = 0;
        private const int REFRESH_INTERVAL = 60;
        private const int LIFE_TICK = 120;

        // 全局哈希系统字典
        public static Dictionary<int, GlobalEnemyHashSystem> planetHashSystems = new Dictionary<int, GlobalEnemyHashSystem>();

        void Awake()
        {
            Log = Logger;

            // 1. 绑定配置
            Cfg_PowerConnect = Config.Bind("1. 信号塔", "电力连接距离", 60.5f, new ConfigDescription("原版: 60.5", new AcceptableValueRange<float>(60.5f, 630f)));
            Cfg_PowerCover = Config.Bind("1. 信号塔", "电力覆盖半径", 14.5f, new ConfigDescription("原版: 14.5", new AcceptableValueRange<float>(14.5f, 630f)));
            Cfg_GroundSignalRange = Config.Bind("1. 信号塔", "地面信号范围", 700f, new ConfigDescription("原版约: 300", new AcceptableValueRange<float>(50f, 5000f)));
            Cfg_SpaceSignalRange = Config.Bind("1. 信号塔", "太空信号范围", 4200f, new ConfigDescription("原版: 4200", new AcceptableValueRange<float>(100f, 50000f)));

            Cfg_BattleBasePickRange = Config.Bind("2. 战场基站", "拾取范围", 90f, new ConfigDescription("原版: 90", new AcceptableValueRange<float>(90f, 630f)));
            Cfg_BattleBaseConstructRange = Config.Bind("2. 战场基站", "建造范围", 60f, new ConfigDescription("原版: 60", new AcceptableValueRange<float>(60f, 630f)));

            Cfg_GaussTurretAttackRange = Config.Bind("3. 炮塔增强", "通用攻击范围", 100000f, new ConfigDescription("高斯/等离子炮射程", new AcceptableValueRange<float>(100f, 1000000f)));
            Cfg_PlasmaMaxSpeed = Config.Bind("3. 炮塔增强", "等离子弹速", 500000f, new ConfigDescription("原版: 20000", new AcceptableValueRange<float>(20000f, 1000000f)));

            Cfg_EnableMarkAll = Config.Bind("4. 开关", "启用全局标记", true, "是否启用信号塔全图标记");
            Cfg_EnablePlasmaTurretPatch = Config.Bind("4. 开关", "启用等离子增强", true, "是否启用等离子炮超视距打击");

            // 2. 绑定事件
            Cfg_PowerConnect.SettingChanged += (s, e) => ApplySettings();
            Cfg_PowerCover.SettingChanged += (s, e) => ApplySettings();
            Cfg_GroundSignalRange.SettingChanged += (s, e) => ApplySettings();
            Cfg_BattleBasePickRange.SettingChanged += (s, e) => ApplySettings();
            Cfg_BattleBaseConstructRange.SettingChanged += (s, e) => ApplySettings();
            Cfg_GaussTurretAttackRange.SettingChanged += (s, e) => ApplySettings();

            new Harmony("com.myself.globalsignaltower").PatchAll();
            Log.LogInfo("【全局信号塔终极版】加载完成！");
        }

        void Update()
        {
            if (GameMain.instance == null || GameMain.isPaused || !GameMain.isRunning) return;

            // 管理哈希系统生命周期
            ManageHashSystems();

            // 降低频率执行逻辑
            update_frame_counter++;
            if (update_frame_counter < REFRESH_INTERVAL) return;
            update_frame_counter = 0;

            MarkAllEnemies();
            RefreshBattleBaseRanges();
        }

        private void ManageHashSystems()
        {
            int currentStarId = GameMain.localStar?.id ?? -1;
            if (currentStarId != -1 && currentStarId != lastStarId)
            {
                lastStarId = currentStarId;
                planetHashSystems.Clear();
                if (GameMain.data?.factoryCount > 0)
                {
                    for (int i = 0; i < GameMain.data.factoryCount; i++)
                    {
                        PlanetFactory factory = GameMain.data.factories[i];
                        if (factory?.planet?.star?.id == currentStarId)
                        {
                            planetHashSystems[factory.planetId] = new GlobalEnemyHashSystem(factory);
                        }
                    }
                }
            }
        }

        // ============================================================
        // 核心功能 1: 信号塔全图标记 (MarkAll)
        // ============================================================
        private void MarkAllEnemies()
        {
            if (!Cfg_EnableMarkAll.Value) return;

            PlanetFactory factory = GameMain.localPlanet?.factory;
            if (factory == null) return;

            // 检查是否有信号塔
            Vector3 beaconPos;
            if (!TryGetBeaconPosition(factory, out beaconPos)) return;

            // 获取哈希系统
            if (!planetHashSystems.TryGetValue(factory.planetId, out var hashSystem)) return;
            if (hashSystem.GetTotalCount() == 0) return;

            DefenseSystem defenseSystem = factory.defenseSystem;
            if (defenseSystem == null) return;

            // 准备参数
            double groundRangeSqr = Math.Pow(Cfg_GroundSignalRange.Value, 2);
            double spaceRangeSqr = Math.Pow(7000.0, 2); // 太空标记硬编码7000，避免过于变态
            int currentAstroId = factory.planet.astroId;

            VectorLF3 beaconPosLF3 = (VectorLF3)beaconPos;
            VectorLF3 beaconWorldPos;
            factory.sector.TransformFromAstro_ref(currentAstroId, out beaconWorldPos, ref beaconPosLF3);

            // 遍历哈希表前4层 (约7000米)
            var hashDatas = hashSystem.GetHashDatas();
            var dataCursors = hashSystem.GetDataCursors();
            int capacity = hashSystem.GetCapacityPerLevel();

            for (int level = 0; level <= 3; level++)
            {
                int start = level * capacity;
                int end = start + dataCursors[level];

                for (int i = start; i < end; i++)
                {
                    var data = hashDatas[i];
                    if (data.id == 0) continue;

                    // 区分地面和太空池
                    EnemyData[] pool = data.isSpaceUnit ? factory.sector.enemyPool : factory.enemyPool;
                    double rangeSqr = data.isSpaceUnit ? spaceRangeSqr : groundRangeSqr;

                    // 只有当 originAstroId 匹配当前星球时，才去地面池找
                    if (!data.isSpaceUnit && data.originAstroId != currentAstroId) continue;

                    if (data.id >= pool.Length) continue;
                    ref EnemyData enemy = ref pool[data.id];

                    if (enemy.id == data.id && !enemy.isInvincible)
                    {
                        VectorLF3 enemyPos;
                        factory.sector.TransformFromAstro_ref(enemy.astroId, out enemyPos, ref enemy.pos);

                        if ((enemyPos - beaconWorldPos).sqrMagnitude <= rangeSqr)
                        {
                            TimedSkillTarget target = default(TimedSkillTarget);
                            target.id = enemy.id;
                            target.astroId = enemy.originAstroId;
                            target.type = ETargetType.Enemy;
                            target.lifeTick = LIFE_TICK;
                            defenseSystem.AddGlobalTargets(ref target);
                        }
                    }
                }
            }
        }

        // ============================================================
        // 辅助方法与补丁
        // ============================================================
        private bool TryGetBeaconPosition(PlanetFactory factory, out Vector3 beaconPos)
        {
            beaconPos = Vector3.zero;
            if (factory.defenseSystem?.beacons?.buffer == null) return false;
            var pool = factory.defenseSystem.beacons.buffer;
            int cursor = factory.defenseSystem.beacons.cursor;
            for (int i = 1; i < cursor; i++)
            {
                if (pool[i].id == i && pool[i].entityId > 0)
                {
                    beaconPos = factory.entityPool[pool[i].entityId].pos;
                    return true;
                }
            }
            return false;
        }

        [HarmonyPatch(typeof(VFPreload), "InvokeOnLoadWorkEnded")]
        class Patch_Init { [HarmonyPostfix] public static void Postfix() => ApplySettings(); }

        public static void ApplySettings()
        {
            // 信号塔
            var signal = LDB.items.Select(3007);
            if (signal?.prefabDesc != null)
            {
                signal.prefabDesc.powerConnectDistance = Cfg_PowerConnect.Value;
                signal.prefabDesc.powerCoverRadius = Cfg_PowerCover.Value;
                signal.prefabDesc.beaconSignalRadius = Cfg_GroundSignalRange.Value;
            }
            // 战场基站
            var bab = LDB.items.Select(3009);
            if (bab?.prefabDesc != null)
            {
                bab.prefabDesc.battleBasePickRange = Cfg_BattleBasePickRange.Value;
                bab.prefabDesc.constructionRange = Cfg_BattleBaseConstructRange.Value;
            }
            // 等离子炮 (ID 3004)
            var plasma = LDB.items.Select(3004);
            if (plasma?.prefabDesc != null)
            {
                plasma.prefabDesc.turretSpaceAttackRange = Cfg_GaussTurretAttackRange.Value;
                plasma.prefabDesc.turretMaxAttackRange = Cfg_GaussTurretAttackRange.Value;
                plasma.prefabDesc.turretPitchUpMax = 90f;
                plasma.prefabDesc.turretPitchDownMax = 20f;
            }
            // 高斯机枪 (ID 3001)
            var gauss = LDB.items.Select(3001);
            if (gauss?.prefabDesc != null)
            {
                gauss.prefabDesc.turretMaxAttackRange = Cfg_GaussTurretAttackRange.Value;
            }
        }

        private void RefreshBattleBaseRanges()
        {
            var factory = GameMain.localPlanet?.factory;
            if (factory?.defenseSystem?.battleBases?.buffer == null) return;
            var pool = factory.defenseSystem.battleBases.buffer;
            int cursor = factory.defenseSystem.battleBases.cursor;
            for (int i = 1; i < cursor; i++)
            {
                if (pool[i].id == i)
                {
                    pool[i].pickRange = Cfg_BattleBasePickRange.Value;
                    pool[i].constructRange = Cfg_BattleBaseConstructRange.Value;
                }
            }
        }

        // ============================================================
        // Harmony Patches
        // ============================================================

        // 1. 驱动哈希系统更新
        [HarmonyPatch(typeof(DFSDynamicHashSystem), "GameTick")]
        public static class Patch_UpdateGlobalHash
        {
            [HarmonyPostfix]
            public static void Postfix(DFSDynamicHashSystem __instance)
            {
                if (__instance.factory != null && planetHashSystems.TryGetValue(__instance.factory.planetId, out var sys))
                {
                    sys.GameTick();
                }
            }
        }

        // 2. 延长子弹寿命 (Transpiler)
        [HarmonyPatch(typeof(TurretComponent), "Shoot_Plasma")]
        public static class Patch_ExtendBulletLife
        {
            static IEnumerable<CodeInstruction> Transpiler(IEnumerable<CodeInstruction> instructions)
            {
                CodeMatcher matcher = new CodeMatcher(instructions);
                var lifeField = AccessTools.Field(typeof(GeneralProjectile), "life");
                var lifeMaxField = AccessTools.Field(typeof(GeneralProjectile), "lifemax");
                int newLife = 12000; // 200秒

                // 修改 life
                matcher.MatchForward(false, new CodeMatch(OpCodes.Stfld, lifeField));
                if (matcher.IsValid)
                {
                    matcher.Advance(-1);
                    if (matcher.Opcode == OpCodes.Ldc_I4_S || matcher.Opcode == OpCodes.Ldc_I4)
                        matcher.SetAndAdvance(OpCodes.Ldc_I4, newLife);
                }

                // 修改 lifemax
                matcher.MatchForward(false, new CodeMatch(OpCodes.Stfld, lifeMaxField));
                if (matcher.IsValid)
                {
                    matcher.Advance(-1);
                    if (matcher.Opcode == OpCodes.Ldc_I4_S || matcher.Opcode == OpCodes.Ldc_I4)
                        matcher.SetAndAdvance(OpCodes.Ldc_I4, newLife);
                }
                return matcher.InstructionEnumeration();
            }
        }

        // 3. 增加子弹速度 (Transpiler)
        [HarmonyPatch(typeof(TurretComponent), "Shoot_Plasma")]
        public static class Patch_IncreasePlasmaSpeed
        {
            static IEnumerable<CodeInstruction> Transpiler(IEnumerable<CodeInstruction> instructions)
            {
                CodeMatcher matcher = new CodeMatcher(instructions);
                while (matcher.MatchForward(false, new CodeMatch(OpCodes.Ldc_R8, 20000.0)).IsValid)
                {
                    matcher.SetOperandAndAdvance((double)Cfg_PlasmaMaxSpeed.Value);
                }
                return matcher.InstructionEnumeration();
            }
        }

        // 4. 接管等离子炮索敌 (核心逻辑)
        [HarmonyPatch(typeof(TurretComponent), "Search_Plasma")]
        public static class Patch_PlasmaSearch
        {
            [HarmonyPostfix]
            public static void Postfix(ref TurretComponent __instance, PlanetFactory factory, PrefabDesc pdesc)
            {
                if (!Cfg_EnablePlasmaTurretPatch.Value) return;
                if (__instance.target.id > 0) return; // 原版已找到

                if (!planetHashSystems.TryGetValue(factory.planetId, out var hashSystem)) return;

                // 检查面板
                var caps = __instance.vsCaps;
                var settings = __instance.vsSettings;
                int checkOrbit = ((int)(caps & settings & VSLayerMask.OrbitHigh)) >> 4;
                int checkSpace = ((int)(caps & settings & VSLayerMask.SpaceHigh)) >> 6;
                if (checkOrbit == 0 && checkSpace == 0) return;

                Vector3 turretPos = factory.entityPool[__instance.entityId].pos;
                float maxRange = pdesc.turretSpaceAttackRange;

                // 查找目标
                IDPOS_Ex targetData = hashSystem.FindNearestEnemy(
                    turretPos, maxRange, checkOrbit > 0, checkSpace > 0, factory
                );

                if (targetData.id > 0)
                {
                    __instance.target.id = targetData.id;
                    __instance.target.astroId = targetData.originAstroId;
                    // 【核心修复】必须设置目标类型，否则不开火
                    __instance.target.type = ETargetType.Enemy;
                }
            }
        }
    }

    // ============================================================
    // 增强版数据结构
    // ============================================================

    // 【修复】增加 isSpaceUnit 标记
    public struct IDPOS_Ex
    {
        public int id;
        public int originAstroId;
        public Vector3 pos;
        public bool isSpaceUnit;

        public IDPOS_Ex(int id, int originAstroId, Vector3 pos, bool isSpace)
        {
            this.id = id;
            this.originAstroId = originAstroId;
            this.pos = pos;
            this.isSpaceUnit = isSpace;
        }
    }

    public class GlobalEnemyHashSystem
    {
        private PlanetFactory factory;
        private SpaceSector sector;
        private PlanetData planetData;

        private const int LEVEL_COUNT = 9;
        public static readonly double[] LEVEL_OFFSETS_SQR = new double[]
        {
            0.0, 4000000.0, 9000000.0, 25000000.0, 49000000.0,
            196000000.0, 784000000.0, 3136000000.0, 10000000000.0
        };
        private const double MAX_DISTANCE_SQR = 10000000000.0;

        private IDPOS_Ex[] hashDatas;
        private IDPOS_Ex[] tmp_datas;
        private int[] dataCursors;
        private int[] tmp_cursors;
        private HashSet<int> old_ids;
        private int groundAddCursor = 1;
        private int capacityPerLevel = 5000;

        public GlobalEnemyHashSystem(PlanetFactory factory)
        {
            this.factory = factory;
            this.sector = factory.sector;
            this.planetData = factory.planet;
            int total = capacityPerLevel * LEVEL_COUNT;
            hashDatas = new IDPOS_Ex[total];
            tmp_datas = new IDPOS_Ex[total];
            dataCursors = new int[LEVEL_COUNT];
            tmp_cursors = new int[LEVEL_COUNT];
            old_ids = new HashSet<int>();
        }

        public void GameTick()
        {
            // 【安全修复】增加对 enemyPool 的检查
            if (planetData == null || sector == null || factory == null || sector.enemyPool == null || factory.enemyPool == null)
            {
                return;
            }

            // 交换缓冲
            var tmpD = tmp_datas; tmp_datas = hashDatas; hashDatas = tmpD;
            var tmpC = tmp_cursors; tmp_cursors = dataCursors; dataCursors = tmpC;

            Array.Clear(hashDatas, 0, hashDatas.Length);
            Array.Clear(dataCursors, 0, dataCursors.Length);

            VectorLF3 uPos = planetData.uPosition;
            int astroId = planetData.astroId;
            old_ids.Clear();

            // 1. 处理旧数据
            for (int level = 0; level < LEVEL_COUNT; level++)
            {
                int start = level * capacityPerLevel;
                int end = start + tmp_cursors[level];
                for (int i = start; i < end; i++)
                {
                    IDPOS_Ex old = tmp_datas[i];
                    if (old.id == 0) continue;

                    // 这里的 TryAddEnemy 现在是安全的了
                    if (TryAddEnemy(old.id, old.originAstroId, uPos, astroId, old.isSpaceUnit))
                    {
                        old_ids.Add(old.id);
                    }
                }
            }

            // 2. 添加新敌人
            AddGroundEnemies(uPos, astroId);
            AddNewEnemies(uPos, astroId);
        }
        private bool TryAddEnemy(int id, int originAstroId, VectorLF3 uPos, int astroId, bool isSpace)
        {
            // 【安全修复 1】检查核心对象是否存在
            if (this.sector == null || this.factory == null) return false;

            EnemyData[] pool = null;

            // 【安全修复 2】分步获取并检查 pool 是否为 null
            if (isSpace)
            {
                pool = this.sector.enemyPool;
            }
            else
            {
                pool = this.factory.enemyPool;
            }

            // 【核心修复】如果 pool 是空的，直接返回，防止访问 pool.Length 时崩溃
            if (pool == null) return false;

            // 检查 ID 越界
            if (id < 0 || id >= pool.Length) return false;

            ref EnemyData enemy = ref pool[id];

            if (enemy.id != id || enemy.isInvincible) return false;

            VectorLF3 wPos;
            // 【安全修复 3】再次检查 sector (虽然前面查过了，但多线程下为了稳妥)
            if (this.sector == null) return false;

            this.sector.TransformFromAstro_ref(enemy.astroId, out wPos, ref enemy.pos);
            double sqrDist = (wPos - uPos).sqrMagnitude;

            if (sqrDist > MAX_DISTANCE_SQR) return false;

            int level = 0;
            for (int i = LEVEL_OFFSETS_SQR.Length - 1; i >= 0; i--)
            {
                if (sqrDist >= LEVEL_OFFSETS_SQR[i]) { level = i; break; }
            }

            if (dataCursors[level] >= capacityPerLevel) return false;

            VectorLF3 lPosLF;
            this.sector.InverseTransformToAstro_ref(astroId, ref wPos, out lPosLF);

            int idx = level * capacityPerLevel + dataCursors[level];
            hashDatas[idx] = new IDPOS_Ex(id, originAstroId, (Vector3)lPosLF, isSpace);
            dataCursors[level]++;
            return true;
        }
        private void AddNewEnemies(VectorLF3 uPos, int astroId)
        {
            if (sector.dfHives == null) return;
            var hive = sector.dfHives[planetData.star.index];
            int starId = planetData.star.id;

            while (hive != null)
            {
                if (!hive.isEmpty && hive.starData.id == starId)
                {
                    // 中继站 (太空单位)
                    var relays = hive.relays.buffer;
                    for (int j = 1; j < hive.relays.cursor; j++)
                    {
                        var r = relays[j];
                        if (r != null && !old_ids.Contains(r.enemyId) && r.id == j && r.stage >= 0)
                            TryAddEnemy(r.enemyId, 0, uPos, astroId, true); // true = 太空
                    }
                    // 舰队 (太空单位)
                    var units = hive.units.buffer;
                    for (int k = 1; k < hive.units.cursor; k++)
                    {
                        var u = units[k];
                        if (!old_ids.Contains(u.enemyId) && u.id == k && u.behavior > EEnemyBehavior.SeekForm)
                            TryAddEnemy(u.enemyId, 0, uPos, astroId, true); // true = 太空
                    }
                }
                hive = hive.nextSibling;
            }
        }

        private void AddGroundEnemies(VectorLF3 uPos, int astroId)
        {
            var pool = factory.enemyPool;
            int cursor = factory.enemyCursor;
            if (cursor <= 1) return;

            int count = cursor / 60 + 1;
            if (count > 200) count = 200;

            for (int i = 0; i < count; i++)
            {
                if (groundAddCursor >= cursor) groundAddCursor = 1;
                ref EnemyData e = ref pool[groundAddCursor];

                // 地面单位检查：originAstroId 必须等于当前星球 ID
                if (e.id == groundAddCursor && !e.isInvincible && !e.isSpace && e.originAstroId == astroId)
                {
                    if (!old_ids.Contains(e.id))
                    {
                        TryAddEnemy(e.id, astroId, uPos, astroId, false); // false = 地面
                        old_ids.Add(e.id);
                    }
                }
                groundAddCursor++;
            }
        }

        public IDPOS_Ex FindNearestEnemy(Vector3 pos, float range, bool relay, bool space, PlanetFactory f)
        {
            float rangeSqr = range * range;
            float minSqr = float.MaxValue;
            IDPOS_Ex best = default;
            var stats = f.skillSystem.combatStats.buffer;
            var spacePool = sector.enemyPool;
            var groundPool = f.enemyPool;

            for (int lvl = LEVEL_COUNT - 1; lvl >= 0; lvl--)
            {
                int start = lvl * capacityPerLevel;
                int end = start + dataCursors[lvl];

                for (int i = start; i < end; i++)
                {
                    IDPOS_Ex d = hashDatas[i];
                    if (d.id == 0) continue;

                    // 【修复】根据标记查池子
                    ref EnemyData e = ref (d.isSpaceUnit ? ref spacePool[d.id] : ref groundPool[d.id]);

                    if (e.id != d.id || e.isInvincible) continue;

                    // 类型过滤
                    bool isRelay = e.dfRelayId > 0;
                    if (d.isSpaceUnit)
                    {
                        if (isRelay && !relay) continue;
                        if (!isRelay && !space) continue;
                    }

                    // 血量过滤
                    if (e.combatStatId > 0 && stats[e.combatStatId].hp <= 0) continue;

                    float distSqr = (d.pos - pos).sqrMagnitude;
                    if (distSqr > rangeSqr) continue;
                    if (Vector3.Dot(pos.normalized, d.pos.normalized) < -0.1f) continue;

                    if (distSqr < minSqr)
                    {
                        minSqr = distSqr;
                        best = d;
                    }
                }
                if (best.id > 0) return best;
            }
            return best;
        }

        public IDPOS_Ex[] GetHashDatas() => hashDatas;
        public int[] GetDataCursors() => dataCursors;
        public int GetCapacityPerLevel() => capacityPerLevel;
        public int GetTotalCount() => dataCursors.Sum();
    }
}